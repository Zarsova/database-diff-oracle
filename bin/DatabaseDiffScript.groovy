import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import groovy.util.logging.Slf4j
import org.apache.poi.hssf.usermodel.HSSFCellStyle
import org.apache.poi.hssf.usermodel.HSSFFont
import org.apache.poi.hssf.usermodel.HSSFHyperlink
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.hssf.util.HSSFColor
import org.apache.poi.ss.usermodel.CellStyle

import java.sql.ResultSetMetaData

/**
 * DatabaseDiff
 *  for oracle database
 */
@Slf4j
class DatabaseDiff {
    final int LIMIT = 1024  // データ比較シートで出力する最大行数
    final int PK_COLUMN = 3 // PKが無い場合先頭から3個のカラムをPKとして取り扱う

    final String SCRIPT_DIR = new File(getClass().protectionDomain.codeSource.location.path).parent
    final String DEFAULT_CONFIG_FILE = new File(new File(SCRIPT_DIR).parent, 'Config.groovy')
    def target = null
    def org = null
    def user = null
    def pass = null
    def hasDiffData = []
    def excludeColumn = []
    def excludeTableColumn = [:]
    def logger = log
    Sql db = null
    def config = null

    public DatabaseDiff(args) {
        def cli = new CliBuilder(usage: 'database-diff [options] [targetSchema] [orgSchema]', header: 'Options:')
        cli.with {
            h(longOpt: 'help', 'print this message')
            c(longOpt: 'config', args: 1, argName: 'config file', "default %SCRIPT_HOME%/Config.groovy")
            u(longOpt: 'user', args: 1, argName: 'username', 'username for database connection')
            p(longOpt: 'pass', args: 1, argName: 'password', 'password for database connection')
        }
        def opt = cli.parse(args)
        if (!opt) System.exit(1)
        if (opt.h) {
            cli.usage()
            System.exit(0)
        }
        def configFile = DEFAULT_CONFIG_FILE
        try {
            if (opt.c) configFile = opt.c
            config = new ConfigSlurper().parse(new File(configFile).toURL())
        } catch (ex) {
            printErr "Error: can't read the config file. path: ${configFile}"
            throw ex
        }
        user = opt.u ? opt.u : config.database.user
        pass = opt.p ? opt.p : config.database.pass
        if (opt.arguments().size() < 2) {
            cli.usage()
            System.exit(0)
        }
        target = (opt.arguments()[0] as String).toUpperCase()
        org = (opt.arguments()[1] as String).toUpperCase()

        if (config.database.url.size() == 0) {
            throw new RuntimeException("Illegal config file. must need database.url\n")
        }
        if (config.exclude.columns.size() > 0) {
            excludeColumn = config.exclude.columns
            excludeColumn.each { logger.info("find exclude column: ${it}") }
        }
        if (config.exclude.table_columns.size() > 0) {
            excludeTableColumn = config.exclude.table_columns
            excludeTableColumn.each {
                it.value.each { column ->
                    logger.info("find exclude table and column: ${it.key}.${column}")
                }
            }
        }
        logger.info "init done - target schema: ${target}, org schema: ${org}"
    }

    def run() {
        logger.info("connect database - url: ${config.database.url}, user: ${user}, pass: ${pass}, driver: ${config.database.driver}")
        db = Sql.newInstance(config.database.url, user, pass, config.database.driver)
        List<String> tTables = rows(db, "SELECT TABLE_NAME FROM dba_tables WHERE owner = '${target}'" as String).collect {
            it.values()
        }.flatten()
        List<String> oTables = rows(db, "SELECT TABLE_NAME FROM dba_tables WHERE owner = '${org}'" as String).collect {
            it.values()
        }.flatten()
        [tTables, oTables].each {
            if (it.size() == 0) {
                throw new RuntimeException("can't find tables in schema: ${tTables.size() == 0 ? target : org}\n")
            }
        }
        def all = (tTables + oTables) as Set
        all = all.sort { it }
        //
        // エクセルへの出力
        //
        def isExclude = { table, column ->
            if (excludeColumn.contains(column)) {
                if (excludeTableColumn.containsKey(table) && !excludeTableColumn[table].contains(column)) {
                    excludeTableColumn[table] << column
                } else {
                    excludeTableColumn[table] = [column]
                }
                return true
            } else if (excludeTableColumn.containsKey(table) && excludeTableColumn[table].contains(column)) {
                return true
            }
            return false
        }.memoize()
        new HSSFWorkbook().with { book ->
            def memFont = { name = "ＭＳ ゴシック", isBold = false, isUnderline = false, color = null ->
                def font = book.createFont()
                font.setFontName(name)
                font.boldweight = (isBold) ? HSSFFont.BOLDWEIGHT_BOLD : HSSFFont.BOLDWEIGHT_NORMAL
                font.underline = (isUnderline) ? HSSFFont.U_SINGLE : HSSFFont.U_NONE
                if (color) {
                    font.setColor(color.index)
                }
                font
            }.memoize()
            def memCellStyle = { border = null, format = null, font = null, bgColor = null, wrapText = false ->
                def style = book.createCellStyle()
                if (border && border instanceof List) {
                    border.each {
                        if (it == 'top') {
                            style.setBorderTop(HSSFCellStyle.BORDER_THIN)
                        } else if (it == 'left') {
                            style.setBorderLeft(HSSFCellStyle.BORDER_THIN)
                        } else if (it == 'right') {
                            style.setBorderRight(HSSFCellStyle.BORDER_THIN)
                        } else if (it == 'bottom') {
                            style.setBorderBottom(HSSFCellStyle.BORDER_THIN)
                        }
                    }
                }
                style.font = (font) ? font : memFont()
                style.wrapText = wrapText
                if (bgColor) {
                    style.fillPattern = CellStyle.SOLID_FOREGROUND
                    style.fillForegroundColor = bgColor.index
                }
                if (format) {
                    style.setDataFormat(book.createDataFormat().getFormat(format))
                }
                style
            }.memoize()

            def tHeaderCellStyle = {
                memCellStyle(['left', 'top', 'right', 'bottom'], null, memFont("ＭＳ ゴシック", true), HSSFColor.LIGHT_GREEN, true)
            }
            def wrapTextCellStyle = {
                memCellStyle(['left', 'top', 'right', 'bottom'], null, null, null, true)
            }
            def tBodyCellStyle = {
                memCellStyle(['left', 'top', 'right', 'bottom'], null, null, null, false)
            }
            def tBodyAlertCellStyle = {
                memCellStyle(['left', 'top', 'right', 'bottom'], null, null, HSSFColor.RED, false)
            }
            def oHeaderCellStyle = {
                memCellStyle(['left', 'top', 'right', 'bottom'], null, memFont("ＭＳ ゴシック", true), HSSFColor.LIGHT_TURQUOISE, true)
            }
            def oBodyCellStyle = {
                memCellStyle(['left', 'top', 'right', 'bottom'], null, null, HSSFColor.LEMON_CHIFFON, false)
            }
            def oBodyAlertCellStyle = {
                memCellStyle(['left', 'top', 'right', 'bottom'], null, null, HSSFColor.ROSE, false)
            }

            // sheet TableStatus
            logger.info("create new sheet - TableStatus")
            createSheet("TableStatus").with { sheet ->
                def headerColumns = ['TABLE_NAME', 'OWNER', 'TABLESPACE_NAME', 'STATUS', 'PCT_FREE',
                                     'PCT_USED', 'INITIAL_EXTENT', 'NEXT_EXTENT', 'MIN_EXTENTS', 'MAX_EXTENTS',
                                     'PCT_INCREASE', 'FREELISTS', 'FREELIST_GROUPS', 'LOGGING', 'NUM_ROWS',
                                     'BLOCKS', 'EMPTY_BLOCKS', 'AVG_ROW_LEN', 'LAST_ANALYZED',]
                def dummyMap = [:]
                headerColumns.each { dummyMap.put(it, '') }
                // sheet TableStatus, row header
                createRow(0).with { row ->
                    createCell(0).with { cell -> cellStyle = tHeaderCellStyle.call(); setCellValue("#") }
                    headerColumns.eachWithIndex { columnName, int columnIdx ->
                        createCell(columnIdx + 1).with { cell -> cellStyle = tHeaderCellStyle.call(); setCellValue(columnName) }
                    }
                    headerColumns.eachWithIndex { columnName, int columnIdx ->
                        createCell(columnIdx + headerColumns.size() + 1).with { cell -> cellStyle = oHeaderCellStyle.call(); setCellValue('ORG_' + columnName) }
                    }
                }
                def tRows = rows(db, "SELECT * FROM dba_tables WHERE owner = '${target}'" as String)
                def oRows = rows(db, "SELECT * FROM dba_tables WHERE owner = '${org}'" as String)
                all.eachWithIndex { tableName, tableIdx ->
                    // sheet TableStatus, row body
                    createRow(tableIdx + 1).with { row ->
                        createCell(0).with { cell ->
                            cellStyle = tBodyCellStyle.call()
                            setCellValue(tableIdx + 1)
                        }
                        def tmpTRows = tRows.findAll { it['TABLE_NAME'] == tableName }
                        def tmpORows = oRows.findAll { it['TABLE_NAME'] == tableName }
                        def tRow = (tmpTRows.size() == 1 ? tmpTRows[0] : dummyMap)
                        def oRow = (tmpORows.size() == 1 ? tmpORows[0] : dummyMap)
                        headerColumns.eachWithIndex { columnName, int idx ->
                            createCell(idx + 1).with { cell ->
                                cellStyle = tBodyCellStyle.call()
                                setCellValue(tRow[columnName])
                                if (tRow[columnName] != oRow[columnName]) {
                                    cellStyle = tBodyAlertCellStyle.call()
                                }
                            }
                        }
                        headerColumns.eachWithIndex { columnName, int idx ->
                            createCell(idx + headerColumns.size() + 1).with { cell ->
                                cellStyle = oBodyCellStyle.call()
                                setCellValue(oRow[columnName])
                                if (tRow[columnName] != oRow[columnName]) {
                                    cellStyle = oBodyAlertCellStyle.call()
                                }
                            }
                        }
                    }
                }
                createFreezePane(1, 1)
            }

            def createSheetByTableName = [:]
            all.eachWithIndex { tableName, tableIdx ->
                try {
                    def pks = primaryKeys(db, tableName, target)
                    def tCols = columns(db, tableName, target)
                    def oCols = columns(db, tableName, org)
                    logger.info "target table: ${tableName} - pks: ${pks}, cols: ${tCols}, orgCols: ${oCols}"
                    def query = """SELECT
${tCols.collect { "t1.${it} AS ${it}" }.join(", ")}, ${oCols.collect { "t2.${it} AS ORG_${it}" }.join(", ")}
FROM ( SELECT ${pks.collect { "tt2.${it}" }.join(", ")}
    FROM ${org}.${tableName} tt2
      LEFT JOIN ${target}.${tableName} tt1
        ON ${pks.collect { "tt1.${it} = tt2.${it}" }.join(" AND ")} UNION
    SELECT ${pks.collect { "tt1.${it}" }.join(", ")}
    FROM ${target}.${tableName} tt1
      LEFT JOIN ${org}.${tableName} tt2
        ON ${pks.collect { "tt1.${it} = tt2.${it}" }.join(" AND ")}
    ORDER BY ${pks.collect { "${it}" }.join(", ")} ) p1
  LEFT JOIN ${target}.${tableName} t1
    ON ${pks.collect { "p1.${it} = t1.${it}" }.join(" AND ")}
  LEFT JOIN ${org}.${tableName} t2
    ON ${pks.collect { "p1.${it} = t2.${it}" }.join(" AND ")}
WHERE rownum <= $LIMIT""" as String
                    def results = rows(db, query)
                    if (results.size() > 0) {
                        // sheet tableName
                        logger.info("create new sheet - ${tableName}")
                        createSheet(tableName).with { sheet ->
                            results.eachWithIndex { GroovyRowResult rowResult, int rowIdx ->
                                if (rowIdx == 0) {
                                    // sheet tableName, row header
                                    createRow(0).with { row ->
                                        rowResult.keySet().eachWithIndex { String key, int columnIdx ->
                                            createCell(columnIdx).with { cell ->
                                                setCellValue(key)
                                                if (key.startsWith("ORG_")) {
                                                    cellStyle = oHeaderCellStyle.call()
                                                } else {
                                                    cellStyle = tHeaderCellStyle.call()
                                                }
                                                if (columnIdx == 0 && rowIdx == 0) {
                                                    cellStyle = memCellStyle(['left', 'top', 'right', 'bottom'], null, memFont("ＭＳ ゴシック", false, true, HSSFColor.BLUE), HSSFColor.LIGHT_GREEN, true)
                                                    def ch = getCreationHelper()
                                                    HSSFHyperlink link = ch.createHyperlink(HSSFHyperlink.LINK_DOCUMENT)
                                                    link.setAddress("AllTables!B${tableIdx + 2}" as String)
                                                    setHyperlink(link)
                                                }
                                            }
                                        }
                                    }
                                }
                                // sheet tableName, row body
                                createRow(rowIdx + 1).with { row ->
                                    rowResult.keySet().eachWithIndex { String key, int columnIdx ->
                                        def val = rowResult[key]
                                        createCell(columnIdx).with { cell ->
                                            cellStyle = tBodyCellStyle.call()
                                            if (!key.startsWith("ORG_")) {
                                                def baseColumnName = key
                                                def oVal = null
                                                try {
                                                    oVal = rowResult["ORG_" + key]
                                                } catch (MissingPropertyException ex) {
                                                    cellStyle = tBodyAlertCellStyle.call()
                                                }
                                                if (isExclude(tableName, baseColumnName)) {
                                                    cellStyle = memCellStyle(['left', 'top', 'right', 'bottom'], null, memFont("ＭＳ ゴシック", false, false, HSSFColor.GREY_25_PERCENT), null, false)
                                                } else if (val != oVal) {
                                                    if (!hasDiffData.contains(tableName)) {
                                                        hasDiffData.add(tableName)
                                                    }
                                                    cellStyle = tBodyAlertCellStyle.call()
                                                }
                                            } else {
                                                def baseColumnName = key.replace("ORG_", "")
                                                cellStyle = oBodyCellStyle.call()
                                                def tVal = null
                                                try {
                                                    tVal = rowResult[baseColumnName]
                                                } catch (MissingPropertyException ex) {
                                                    cellStyle = oBodyAlertCellStyle.call()
                                                }
                                                if (isExclude(tableName, baseColumnName)) {
                                                    cellStyle = memCellStyle(['left', 'top', 'right', 'bottom'], null, memFont("ＭＳ ゴシック", false, false, HSSFColor.GREY_25_PERCENT), HSSFColor.LEMON_CHIFFON, false)
                                                } else if (val != tVal) {
                                                    cellStyle = oBodyAlertCellStyle.call()
                                                }
                                            }
                                            setCellValue("${val}")
                                        }
                                    }
                                }
                            }
                            createFreezePane(0, 1, 0, 1);
                            createSheetByTableName[tableName] = sheet.sheetName
                        }
                    }
                } catch (IllegalArgumentException iaex) {
                    logger.error "Error in ${tableName}"
                }
            }
            // sheet AllTables
            logger.info("create new sheet - AllTables")
            createSheet("AllTables").with { sheet ->
                // sheet AllTables, row header
                createRow(0).with { row ->
                    ["#", "テーブル名", "${target}", "${target}(件数)", "${org}", "${org}(件数)", "件数差異なし", "データ差異なし", "無視するカラム"].eachWithIndex { String key, int idx ->
                        createCell(idx).with { cell ->
                            setCellValue(key)
                            cellStyle = tHeaderCellStyle.call()
                        }
                    }
                }
                all.eachWithIndex { tableName, tableIdx ->
                    def targetCount = null
                    def orgCount = null
                    if (tTables.contains(tableName)) {
                        targetCount = firstRow(db, 'SELECT count(*) AS count FROM ' + "${target}." + tableName).count
                    }
                    if (oTables.contains(tableName)) {
                        orgCount = firstRow(db, 'SELECT count(*) AS count FROM ' + "${org}." + tableName).count
                    }

                    // sheet AllTables, row body
                    createRow(tableIdx + 1).with { row ->
                        def exclude_msg = {
                            if (excludeTableColumn.containsKey(tableName)) {
                                excludeTableColumn[tableName].join(",")
                            } else {
                                ""
                            }
                        }

                        [tableIdx + 1, tableName, tTables.contains(tableName), targetCount, oTables.contains(tableName), orgCount, targetCount == orgCount, !hasDiffData.contains(tableName), exclude_msg.call()].eachWithIndex { val, int idx ->
                            createCell(idx).with { cell ->
                                cellStyle = tBodyCellStyle.call()
                                if (idx == 2 && tTables.contains(tableName) != oTables.contains(tableName)) {
                                    cellStyle = tBodyAlertCellStyle.call()
                                } else if (idx == 3 && targetCount != orgCount) {
                                    cellStyle = tBodyAlertCellStyle.call()
                                } else if (val instanceof Boolean && val == false) {
                                    cellStyle = tBodyAlertCellStyle.call()
                                } else if (idx == 8) {
                                    cellStyle = wrapTextCellStyle.call()
                                }
                                if (val != "") {
                                    setCellValue(val)
                                }
                                if (idx == 1 && createSheetByTableName[tableName]) {
                                    cellStyle = memCellStyle(['left', 'top', 'right', 'bottom'], null, memFont("ＭＳ ゴシック", false, true, HSSFColor.BLUE), null, false)
                                    def ch = getCreationHelper()
                                    HSSFHyperlink link = ch.createHyperlink(HSSFHyperlink.LINK_DOCUMENT)
                                    link.setAddress((createSheetByTableName[tableName] + '!A1') as String)
                                    setHyperlink(link)
                                }
                            }
                        }
                    }
                }
                createFreezePane(1, 1);
            }

            setSheetOrder("AllTables", 0)
            new File("xlsout").mkdirs()
            def fileName = "xlsout/DatabaseDiff_${target}-${org}_${new Date().format('yyyyMMdd-HHmmss')}.xls"
            logger.info("create xls file - ${fileName}")
            new File(fileName).withOutputStream { os ->
                write(os)
            }
        }
    }

    def primaryKeys = { Sql sql, table, schema ->
        def keys = []
        rows(sql, "SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner\n" +
                "FROM dba_constraints cons, dba_cons_columns cols\n" +
                "WHERE cols.table_name = cols.table_name\n" +
                "AND  cols.table_name = '${table}'\n" +
                "AND cons.constraint_type = 'P'\n" +
                "AND cons.constraint_name = cols.constraint_name\n" +
                "AND cons.owner = cols.owner\n" +
                "AND cons.owner = '${schema}'\n" +
                "ORDER BY cols.table_name, cols.position" as String).each { row ->
            keys << row["COLUMN_NAME"]
        }

        if (keys.size() > 0) {
            return keys
        } else {
            def result = []
            def count = 0
            rows(sql, "SELECT * FROM DBA_TAB_COLS cols\n" +
                    "WHERE  Cols.Owner = '${schema}'\n" +
                    "and Cols.table_name = '${table}'\n" +
                    "order by Cols.Column_Id" as String).each { row ->
                if (count <= PK_COLUMN) {
                    result << row["COLUMN_NAME"]
                }
                count += 1
            }
            result
        }
    }

    def rows = { Sql sql, String query ->
        logger.debug "query - " + query.replace("\n", " ").replace("\r", " ").replaceAll(" +", " ")
        sql.rows(query)
    }

    def firstRow = { Sql sql, String query ->
        logger.debug "query - " + query.replace("\n", " ").replace("\r", " ").replaceAll(" +", " ")
        sql.firstRow(query)
    }

    def columns = { Sql sql, String table, String schema ->
        def ret = []
        def query = "SELECT * FROM ${schema}." + table + " WHERE rownum = 1" as String
        logger.debug "query - " + query.replace("\n", " ").replace("\r", " ").replaceAll(" +", " ")
        sql.rows(query) { ResultSetMetaData meta ->
            ret = (1..meta.columnCount).collect {
                meta.getColumnName(it)
            }
        }
        ret
    }

    def printErr = System.err.&println

    public static main(args) {
        def runner = null
        try {
            runner = new DatabaseDiff(args)
            runner.run()
        } catch (ex) {
            System.err << "Error: ${ex.message}"
            ex.stackTrace.each { StackTraceElement ste ->
                if (ste.toString().contains('DatabaseDiff')) System.err << "\t at ${ste.toString()}\n"
            }
            System.exit(1)
        } finally {
            if (runner != null && runner.db != null) {
                runner.db.close()
            }
        }
    }
}

def runner = null
try {
    runner = new DatabaseDiff(args)
    runner.run()
} catch (ex) {
    System.err << "Error: ${ex.message}"
    ex.stackTrace.each { StackTraceElement ste ->
        if (ste.toString().contains('DatabaseDiff')) System.err << "\t at ${ste.toString()}\n"
    }
    System.exit(1)
} finally {
    if (runner != null && runner.db != null) {
        runner.db.close()
    }
}
