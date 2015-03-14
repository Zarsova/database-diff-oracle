import groovy.sql.Sql
import groovy.util.logging.Slf4j
import org.apache.poi.hssf.usermodel.HSSFCellStyle
import org.apache.poi.hssf.usermodel.HSSFFont
import org.apache.poi.hssf.usermodel.HSSFHyperlink
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.hssf.util.HSSFColor
import org.apache.poi.ss.usermodel.CellStyle

import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.SQLSyntaxErrorException

/**
 * DatabaseDiff
 *  for oracle database
 */
@Slf4j
class DatabaseDiff {
    def PREFIX = "Z_"
    def RECORD_COUNT = "ZZ_RECORD_COUNT"

    def limit
    def pkColumn
    def user
    def pass
    def config
    def hasDiffData = []
    def excludeColumn = []
    def excludeTableColumn = [:]
    def logger = log
    Sql db = null

    def run(String target, String org) {
        logger.info "init args - user: ${user}, pass: ${pass}, limit: ${limit}, pkColumn ${pkColumn}"
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

        logger.info("connect database - url: ${config.database.url}, user: ${user}, pass: ${pass}, driver: ${config.database.driver}")
        def dateString = new Date().format('yyyyMMdd-HHmmss')
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
        def allTables = (tTables + oTables) as Set
        allTables = allTables.sort { it }
        def isMemExclude = { table, column ->
            isExclude(table, column)
        }.memoize()
        // エクセルへの出力
        new HSSFWorkbook().with { book ->
            def memFont = { name = "ＭＳ ゴシック", isBold = false, isUnderline = false, color = null ->
                def font = book.createFont()
                font.setFontName(name)
                font.boldweight = (isBold) ? HSSFFont.BOLDWEIGHT_BOLD : HSSFFont.BOLDWEIGHT_NORMAL
                font.underline = (isUnderline) ? HSSFFont.U_SINGLE : HSSFFont.U_NONE
                if (color) font.setColor(color.index)
                font
            }.memoize()
            def memCellStyle = { border = null, format = null, font = null, bgColor = null, wrapText = false ->
                def style = book.createCellStyle()
                if (border && border instanceof List) {
                    border.each {
                        if (it == 'top') style.setBorderTop(HSSFCellStyle.BORDER_THIN)
                        else if (it == 'left') style.setBorderLeft(HSSFCellStyle.BORDER_THIN)
                        else if (it == 'right') style.setBorderRight(HSSFCellStyle.BORDER_THIN)
                        else if (it == 'bottom') style.setBorderBottom(HSSFCellStyle.BORDER_THIN)
                    }
                }
                style.font = (font) ? font : memFont()
                style.wrapText = wrapText
                if (bgColor) {
                    style.fillPattern = CellStyle.SOLID_FOREGROUND
                    style.fillForegroundColor = bgColor.index
                }
                if (format) style.setDataFormat(book.createDataFormat().getFormat(format))
                style
            }.memoize()

            def cellStyles = [
                    tHeader     : memCellStyle(['left', 'top', 'right', 'bottom'], null, memFont("ＭＳ ゴシック", true), HSSFColor.LIGHT_GREEN, true),
                    wrapText    : memCellStyle(['left', 'top', 'right', 'bottom'], null, null, null, true),
                    tHeaderLink : memCellStyle(['left', 'top', 'right', 'bottom'], null, memFont("ＭＳ ゴシック", false, true, HSSFColor.BLUE), HSSFColor.LIGHT_GREEN, true),
                    tBody       : memCellStyle(['left', 'top', 'right', 'bottom'], null, null, null, false),
                    tBodyAlert  : memCellStyle(['left', 'top', 'right', 'bottom'], null, null, HSSFColor.RED, false),
                    tBodyExclude: memCellStyle(['left', 'top', 'right', 'bottom'], null, memFont("ＭＳ ゴシック", false, false, HSSFColor.GREY_25_PERCENT), null, false),
                    oHeader     : memCellStyle(['left', 'top', 'right', 'bottom'], null, memFont("ＭＳ ゴシック", true), HSSFColor.LIGHT_TURQUOISE, true),
                    oBody       : memCellStyle(['left', 'top', 'right', 'bottom'], null, null, HSSFColor.LEMON_CHIFFON, false),
                    oBodyAlert  : memCellStyle(['left', 'top', 'right', 'bottom'], null, null, HSSFColor.ROSE, false),
                    oBodyExclude: memCellStyle(['left', 'top', 'right', 'bottom'], null, memFont("ＭＳ ゴシック", false, false, HSSFColor.GREY_25_PERCENT), HSSFColor.LEMON_CHIFFON, false)
            ]

            def createSheetByTableName = [:]

            allTables.eachWithIndex { String tableName, int tableIdx ->
                try {
                    def allRowMode = true
                    def usedSubStr = []
                    def subStr = { String str ->
                        def rtn = str.length() > (30 - PREFIX.length() - 2) ?
                                str.substring(0, (30 - PREFIX.length() - 2)) :
                                str
                        usedSubStr += rtn
                        (str == rtn ? rtn : rtn + "_${usedSubStr.count(rtn)}")
                    }.memoize()
                    def pks = primaryKeys(db, tableName, target)
                    def tCols = columns(db, tableName, target)
                    def oCols = columns(db, tableName, org)
                    logger.info "target table: ${tableName} - pks: ${pks}, cols: ${tCols}, orgCols: ${oCols}"
                    def query = """SELECT COUNT(*) over() AS ${RECORD_COUNT},
${tCols.collect { "t1.${it} AS ${subStr(it)}" }.join(", ")},
${oCols.collect { "t2.${it} AS Z_${subStr(it)}" }.join(", ")}
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
    ON ${pks.collect { "p1.${it} = t2.${it}" }.join(" AND ")}""" as String
                    //WHERE rownum <= $limit
                    // sheet tableName
                    logger.info("create new xls file - ${tableName}")
                    logger.debug "query - " + query.replace("\n", " ").replace("\r", " ").replaceAll(" +", " ")
                    db.query(query) { ResultSet resultSet ->
                        int rowIdx = 0
                        int rsCount = 0
                        boolean headerOut = false
                        try {
                            createSheet(tableName).with { sheet ->
                                def columnNames = []
                                def tColumns = []
                                def oColumns = []
                                ResultSetMetaData rowResultMeta = resultSet.getMetaData()
                                for (int i = 1; i < rowResultMeta.columnCount; i++) {
                                    def columnName = rowResultMeta.getColumnName(i + 1)
                                    columnNames.add(columnName)
                                    if (columnName.startsWith(PREFIX)) {
                                        if (!isMemExclude(tableName, columnName.substring(PREFIX.length()))) {
                                            oColumns.add(i + 1)
                                        }
                                    } else {
                                        if (!isMemExclude(tableName, columnName)) {
                                            tColumns.add(i + 1)
                                        }
                                    }
                                }
                                while (resultSet.next()) {
                                    if (!headerOut) {
                                        // sheet tableName, row header
                                        createRow(rowIdx).with { row ->
                                            columnNames.eachWithIndex { String key, int columnIdx ->
                                                createCell(columnIdx).with { cell ->
                                                    setCellValue(key)
                                                    if (key.startsWith(PREFIX)) cellStyle = cellStyles.oHeader
                                                    else cellStyle = cellStyles.tHeader
                                                    // 先頭にハイパーリンクを埋め込む
                                                    if (columnIdx == 0) {
                                                        cellStyle = cellStyles.tHeaderLink
                                                        HSSFHyperlink link = getCreationHelper().createHyperlink(HSSFHyperlink.LINK_FILE)
                                                        link.setAddress("DatabaseDiff_${target}-${org}_${dateString}.xls" as String)
                                                        setHyperlink(link)
                                                    }
                                                }
                                            }
                                        }
                                        rowIdx++
                                        createFreezePane(0, 1, 0, 1);
                                        createSheetByTableName[tableName] = sheet.sheetName
                                        headerOut = true
                                        if (resultSet.getInt(RECORD_COUNT) > limit) {
                                            logger.info("table: ${tableName}, recored count: ${resultSet.getInt(RECORD_COUNT)} - enable diff mode")
                                            allRowMode = false
                                        }
                                    }
                                    // sheet tableName, row body
                                    boolean out = false
                                    if (!allRowMode) {
                                        def tData = tColumns.collect { resultSet.getString(it) }
                                        def oData = oColumns.collect { resultSet.getString(it) }
                                        out = (tData == oData ? false : true)
                                    }
                                    if (allRowMode || out) {
                                        createRow(rowIdx).with { row ->
                                            columnNames.eachWithIndex { String key, int columnIdx ->
                                                def val = resultSet.getString(key)
                                                createCell(columnIdx).with { cell ->
                                                    if (!key.startsWith(PREFIX)) {
                                                        cellStyle = cellStyles.tBody
                                                        if (isMemExclude(tableName, key)) {
                                                            cellStyle = cellStyles.tBodyExclude
                                                        } else {
                                                            try {
                                                                if (val != resultSet.getString(PREFIX + key)) {
                                                                    if (!hasDiffData.contains(tableName)) hasDiffData.add(tableName)
                                                                    cellStyle = cellStyles.tBodyAlert
                                                                }
                                                            } catch (SQLException ex) {
                                                                cellStyle = cellStyles.tBodyAlert
                                                            }
                                                        }
                                                    } else {
                                                        cellStyle = cellStyles.oBody
                                                        def baseColumnName = key.substring(PREFIX.length())
                                                        if (isMemExclude(tableName, baseColumnName)) {
                                                            cellStyle = cellStyles.oBodyExclude
                                                        } else {
                                                            try {
                                                                if (val != resultSet.getString(baseColumnName)) {
                                                                    cellStyle = cellStyles.oBodyAlert
                                                                }
                                                            } catch (SQLException ex) {
                                                                cellStyle = cellStyles.oBodyAlert
                                                            }
                                                        }
                                                    }
                                                    setCellValue("${val}")
                                                }
                                            }
                                        }
                                        rowIdx++
                                    }
                                    rsCount++
                                    if ((rsCount) % 10000 == 0) logger.info("create xls: ${tableName}, row: ${rsCount}, output: ${rowIdx}${(allRowMode ? "" : ", diff mode")}")
                                }
                            }
                        } catch (IllegalArgumentException e) {
                            logger.error "Error in ${tableName} (IllegalArgumentException: ${e})"
                            e.printStackTrace()
                        }

                        if (rowIdx > 0) {
                            new File("xlsout").mkdirs()
                            def fileName = "xlsout/DatabaseDiff_${target}-${org}_${dateString}_${tableName}.xls"
                            logger.info("create xls file - ${fileName}")
                            new File(fileName).withOutputStream { os ->
                                write(os)
                            }
                            book.removeSheetAt(0)
                        }
                    }
                } catch (SQLSyntaxErrorException e) {
                    logger.error "Error in ${tableName} (SQLSyntaxErrorException: ${false})"
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
                            cellStyle = cellStyles.tHeader
                        }
                    }
                }
                allTables.eachWithIndex { tableName, tableIdx ->
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
                                cellStyle = cellStyles.tBody
                                if (idx == 2 && tTables.contains(tableName) != oTables.contains(tableName)) {
                                    cellStyle = cellStyles.tBodyAlert
                                } else if (idx == 3 && targetCount != orgCount) {
                                    cellStyle = cellStyles.tBodyAlert
                                } else if (val instanceof Boolean && val == false) {
                                    cellStyle = cellStyles.tBodyAlert
                                } else if (idx == 8) {
                                    cellStyle = cellStyles.wrapText
                                }
                                if (val != "") {
                                    setCellValue(val)
                                }
                                if (idx == 1 && createSheetByTableName[tableName]) {
                                    cellStyle = memCellStyle(['left', 'top', 'right', 'bottom'], null, memFont("ＭＳ ゴシック", false, true, HSSFColor.BLUE), null, false)
                                    def ch = getCreationHelper()
                                    //HSSFHyperlink link = ch.createHyperlink(HSSFHyperlink.LINK_DOCUMENT)
                                    //link.setAddress((createSheetByTableName[tableName] + '!A1') as String)
                                    HSSFHyperlink link = ch.createHyperlink(HSSFHyperlink.LINK_FILE)
                                    link.setAddress("DatabaseDiff_${target}-${org}_${dateString}_${tableName}.xls" as String)
                                    setHyperlink(link)
                                }
                            }
                        }
                    }
                }
                createFreezePane(1, 1);
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
                    createCell(0).with { cell -> cellStyle = cellStyles.tHeader; setCellValue("#") }
                    headerColumns.eachWithIndex { columnName, int columnIdx ->
                        createCell(columnIdx + 1).with { cell -> cellStyle = cellStyles.tHeader; setCellValue(columnName) }
                    }
                    headerColumns.eachWithIndex { columnName, int columnIdx ->
                        createCell(columnIdx + headerColumns.size() + 1).with { cell -> cellStyle = cellStyles.oHeader; setCellValue('Z' + columnName) }
                    }
                }
                def tRows = rows(db, "SELECT * FROM dba_tables WHERE owner = '${target}'" as String)
                def oRows = rows(db, "SELECT * FROM dba_tables WHERE owner = '${org}'" as String)
                allTables.eachWithIndex { tableName, tableIdx ->
                    // sheet TableStatus, row body
                    createRow(tableIdx + 1).with { row ->
                        createCell(0).with { cell ->
                            cellStyle = cellStyles.tBody
                            setCellValue(tableIdx + 1)
                        }
                        def tmpTRows = tRows.findAll { it['TABLE_NAME'] == tableName }
                        def tmpORows = oRows.findAll { it['TABLE_NAME'] == tableName }
                        def tRow = (tmpTRows.size() == 1 ? tmpTRows[0] : dummyMap)
                        def oRow = (tmpORows.size() == 1 ? tmpORows[0] : dummyMap)
                        headerColumns.eachWithIndex { columnName, int idx ->
                            createCell(idx + 1).with { cell ->
                                cellStyle = cellStyles.tBody
                                setCellValue(tRow[columnName])
                                if (tRow[columnName] != oRow[columnName]) {
                                    cellStyle = cellStyles.tBodyAlert
                                }
                            }
                        }
                        headerColumns.eachWithIndex { columnName, int idx ->
                            createCell(idx + headerColumns.size() + 1).with { cell ->
                                cellStyle = cellStyles.oBody
                                setCellValue(oRow[columnName])
                                if (tRow[columnName] != oRow[columnName]) {
                                    cellStyle = cellStyles.oBodyAlert
                                }
                            }
                        }
                    }
                }
                createFreezePane(1, 1)
            }

            setSheetOrder("AllTables", 0)
            new File("xlsout").mkdirs()
            def fileName = "xlsout/DatabaseDiff_${target}-${org}_${dateString}.xls"
            logger.info("create xls file - ${fileName}")
            new File(fileName).withOutputStream { os ->
                write(os)
            }
        }
    }

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
    }

    def primaryKeys = { Sql sql, table, schema ->
        def keys = []
        rows(sql, "SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner\n" +
                "FROM dba_constraints cons, dba_cons_columns cols\n" +
                "WHERE cols.table_name = cols.table_name\n" +
                "AND cols.table_name = '${table}'\n" +
                "AND cons.constraint_type = 'P'\n" +
                "AND cons.constraint_name = cols.constraint_name\n" +
                "AND cons.owner = cols.owner\n" +
                "AND cons.owner = '${schema}'\n" +
                "ORDER BY cols.table_name, cols.position" as String).each { row ->
            if (!isExclude(table, row["COLUMN_NAME"])) {
                keys << row["COLUMN_NAME"]
            }
        }

        if (keys.size() > 0) {
            return keys
        } else {
            def result = []
            def count = 0
            rows(sql, "SELECT * FROM DBA_TAB_COLS cols\n" +
                    "WHERE Cols.Owner = '${schema}'\n" +
                    "and Cols.table_name = '${table}'\n" +
                    "order by Cols.Column_Id" as String).each { row ->
                if (count <= pkColumn) {
                    if (!isExclude(table, row["COLUMN_NAME"])) {
                        result << row["COLUMN_NAME"]
                        count += 1
                    }
                }
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
        def query = "SELECT * FROM ${schema}.${table} WHERE rownum = 1" as String
        logger.debug "query - " + query.replace("\n", " ").replace("\r", " ").replaceAll(" +", " ")
        sql.rows(query) { ResultSetMetaData meta ->
            ret = (1..meta.columnCount).collect {
                meta.getColumnName(it)
            }
        }
        ret
    }

    static printErr = System.err.&println

    public static main(args, scriptDir) {
        def cli = new CliBuilder(usage: 'database-diff [options] [targetSchema] [orgSchema]', header: 'Options:')
        cli.with {
            h(longOpt: 'help', 'print this message')
            c(longOpt: 'config', args: 1, argName: 'config file', "default %SCRIPT_HOME%/Config.groovy")
            u(longOpt: 'user', args: 1, argName: 'username', 'username for database connection')
            p(longOpt: 'pass', args: 1, argName: 'password', 'password for database connection')
            l(longOpt: 'limit', args: 1, argName: 'limit', 'limit count of all row mode')
        }
        def opt = cli.parse(args)
        if (!opt) System.exit(1)
        if (opt.h) {
            cli.usage()
            System.exit(0)
        }
        if (opt.l) {
            try {
                Integer.parseInt(opt.l)
            } catch (e) {
                printErr "Error: limit need integer. current: ${opt.l}"
                System.exit(1)
            }
        }
        def configFile = new File(new File(scriptDir).parent, 'Config.groovy').path
        def config
        try {
            if (opt.c) {
                configFile = opt.c
            }
            config = new ConfigSlurper().parse(new File(configFile).toURL())
        } catch (e) {
            printErr "Error: can't read the config file. path: ${configFile}"
            throw e
        }
        if (opt.arguments().size() < 2) {
            cli.usage()
            System.exit(0)
        }
        def runner
        try {
            runner = new DatabaseDiff(
                    user: (opt.u ? opt.u : config.database.user),
                    pass: (opt.p ? opt.p : config.database.pass),
                    limit: (opt.l ? Integer.parseInt(opt.l) : 65535),  // データ比較シートで出力する最大行数
                    pkColumn: 3, // PKが無い場合先頭から3個のカラムをPKとして取り扱う
                    config: config
            )
            runner.run(
                    (opt.arguments()[0] as String).toUpperCase(), (opt.arguments()[1] as String).toUpperCase()
            )
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

DatabaseDiff.main(args, new File(getClass().protectionDomain.codeSource.location.path).parent)