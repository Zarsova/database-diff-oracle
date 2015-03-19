import groovy.sql.Sql
import groovy.util.logging.Slf4j
import org.apache.poi.hssf.usermodel.*
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
    def XLS_SUFFIX = new Date().format('yyyyMMdd-HHmmss')
    def XLS_PREFIX = 'DbDiff_'
    def XLS_OUTPUT_DIR = 'xlsout'
    def limit
    def pkColumn
    def user
    def pass
    def config
    def tableWithDiffAry = []
    def includeTable = []
    def excludeColumn = []
    def excludeTableColumn = [:]
    def logger = log
    Sql db = null

    def run(String target, String org) {
        logger.info "Init args - target schema: ${target}, org schema: ${org}, user: ${user}, pass: ${pass}, limit: ${limit}, pk column: ${pkColumn}"
        if (config.database.url.size() == 0) {
            throw new RuntimeException("Illegal config file. must need database.url\n")
        }
        if (config.exclude.columns.size() > 0) {
            excludeColumn = config.exclude.columns.collect { it.toUpperCase() }
            logger.info("Find exclude column: ${excludeColumn.join(', ')}")
        }
        if (config.include.tables.size() > 0) {
            includeTable = config.include.tables.collect { it.toUpperCase() }
            logger.info("Find include table: ${includeTable.join(', ')}")
        }
        if (config.exclude.table_columns.size() > 0) {
            config.exclude.table_columns.each {
                excludeTableColumn[it.key.toUpperCase()] = it.value.collect { it.toUpperCase() }
            }
            excludeTableColumn.each {
                logger.info("Find exclude table and column: ${it.value.collect { column -> "${it.key}.${column}" }.join(', ')}")
            }
        }
        logger.info "Init done"
        logger.info("Connect database - url: ${config.database.url}, user: ${user}, pass: ${pass}, driver: ${config.database.driver}")
        db = Sql.newInstance(config.database.url, user, pass, config.database.driver)
        def tTableAry = tablesByOwner(db, target)
        def oTableAry = tablesByOwner(db, org)
        [tTableAry, oTableAry].each {
            if (it.size() == 0) {
                throw new RuntimeException("Can't find tables in schema: ${tTableAry.size() == 0 ? target : org}\n")
            }
        }
        def allTableAry = (tTableAry + oTableAry) as Set
        allTableAry = allTableAry.sort { it }
        // エクセルへの出力
        new HSSFWorkbook().with { HSSFWorkbook book ->
            def memFont = { fontName = "ＭＳ ゴシック", isBold = false, isUnderline = false, color = null ->
                def font = book.createFont()
                font.setFontName(fontName)
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
            def cellStyleMap = [
                    tHeader     : memCellStyle(['left', 'top', 'right', 'bottom'], null, memFont("ＭＳ ゴシック", true), HSSFColor.LIGHT_GREEN, true),
                    wrapText    : memCellStyle(['left', 'top', 'right', 'bottom'], null, null, null, true),
                    tHeaderLink : memCellStyle(['left', 'top', 'right', 'bottom'], null, memFont("ＭＳ ゴシック", false, true, HSSFColor.BLUE), HSSFColor.LIGHT_GREEN, true),
                    tBodyLink   : memCellStyle(['left', 'top', 'right', 'bottom'], null, memFont("ＭＳ ゴシック", false, true, HSSFColor.BLUE), null, false),
                    tBody       : memCellStyle(['left', 'top', 'right', 'bottom'], null, null, null, false),
                    tBodyAlert  : memCellStyle(['left', 'top', 'right', 'bottom'], null, null, HSSFColor.RED, false),
                    tBodyExclude: memCellStyle(['left', 'top', 'right', 'bottom'], null, memFont("ＭＳ ゴシック", false, false, HSSFColor.GREY_25_PERCENT), null, false),
                    oHeader     : memCellStyle(['left', 'top', 'right', 'bottom'], null, memFont("ＭＳ ゴシック", true), HSSFColor.LIGHT_TURQUOISE, true),
                    oBody       : memCellStyle(['left', 'top', 'right', 'bottom'], null, null, HSSFColor.LEMON_CHIFFON, false),
                    oBodyAlert  : memCellStyle(['left', 'top', 'right', 'bottom'], null, null, HSSFColor.ROSE, false),
                    oBodyExclude: memCellStyle(['left', 'top', 'right', 'bottom'], null, memFont("ＭＳ ゴシック", false, false, HSSFColor.GREY_25_PERCENT), HSSFColor.LEMON_CHIFFON, false)
            ]

            def linkTableNames = [:]
            allTableAry.eachWithIndex { String tableName, int tableIdx ->
                // sheet tableName
                logger.info("Create new xls file - ${tableName}")
                try {
                    def replaceCols = [:]
                    db.query(createUnionQuery(db, tableName, target, org, replaceCols)) { ResultSet resultSet ->
                        def memIsExclude = { String column -> isExclude(tableName, column) }.memoize()
                        def allRowMode = true
                        int cursorIdx = 0
                        int rowIdx = 0
                        boolean headerOutDone = false
                        try {
                            book.createSheet(tableName).with { HSSFSheet sheet ->
                                def columnPropMap = [:]
                                def columnNameAry = []
                                def tColIdxAry = []
                                def oColIdxAry = []
                                ResultSetMetaData rowResultMeta = resultSet.getMetaData()
                                for (int i = 1; i < rowResultMeta.columnCount; i++) {
                                    columnNameAry.add(rowResultMeta.getColumnName(i + 1))
                                }
                                columnNameAry.eachWithIndex { String columnName, int idx ->
                                    // create columnPropMap
                                    def _isTarget = !columnName.startsWith(PREFIX)
                                    def _isOrg = columnName.startsWith(PREFIX)
                                    def _isExclude = memIsExclude(replaceCols[columnName])
                                    def _columnIndex = idx + 2
                                    def _otherColumnIndex = _isTarget ?
                                            columnNameAry.indexOf(PREFIX + columnName) :
                                            columnNameAry.indexOf(columnName.substring(PREFIX.length()))
                                    _otherColumnIndex = (_otherColumnIndex == -1) ? null : _otherColumnIndex + 2
                                    columnPropMap[columnName] = [isTarget        : _isTarget,
                                                                 isOrg           : _isOrg,
                                                                 isExclude       : _isExclude,
                                                                 columnIndex     : _columnIndex,
                                                                 otherColumnIndex: _otherColumnIndex,]
                                    // create tColIndexes and oColIndexes
                                    if (!_isExclude && _isOrg)
                                        oColIdxAry.add(_columnIndex)
                                    if (!_isExclude && _isTarget)
                                        tColIdxAry.add(_columnIndex)
                                }
                                while (resultSet.next()) {
                                    if (!headerOutDone) {
                                        sheet.createRow(rowIdx).with { HSSFRow row ->
                                            columnNameAry.eachWithIndex { String columnName, int columnIdx ->
                                                Map prop = columnPropMap[columnName]
                                                row.createCell(columnIdx).with { HSSFCell cell ->
                                                    cell.setCellValue(replaceCols[columnName])
                                                    cell.cellStyle = prop.isTarget ? cellStyleMap.tHeader : cellStyleMap.oHeader
                                                    // 先頭にハイパーリンクを埋め込む
                                                    if (columnIdx == 0) {
                                                        cell.cellStyle = cellStyleMap.tHeaderLink
                                                        HSSFHyperlink link = book.getCreationHelper().createHyperlink(HSSFHyperlink.LINK_FILE)
                                                        link.setAddress("${XLS_PREFIX}${target}-${org}_${XLS_SUFFIX}.xls" as String)
                                                        cell.setHyperlink(link)
                                                    }
                                                }
                                            }
                                        }
                                        rowIdx++
                                        sheet.createFreezePane(0, 1, 0, 1);
                                        linkTableNames[tableName] = sheet.sheetName
                                        // sheet tableName, row header
                                        headerOutDone = true
                                        if (resultSet.getInt(RECORD_COUNT) > limit) {
                                            logger.info("table: ${tableName}, recored count: ${resultSet.getInt(RECORD_COUNT)} - enable diff mode")
                                            allRowMode = false
                                        }
                                    }
                                    // sheet tableName, row body
                                    boolean isRowWithDiff = false
                                    if (!allRowMode) {
                                        def _tAry = tColIdxAry.collect { resultSet.getString(it) }
                                        def _oAry = oColIdxAry.collect { resultSet.getString(it) }
                                        isRowWithDiff = (_tAry != _oAry)
                                    }
                                    if (allRowMode || isRowWithDiff) {
                                        sheet.createRow(rowIdx).with { HSSFRow row ->
                                            columnNameAry.eachWithIndex { String columnName, int columnIdx ->
                                                Map prop = columnPropMap[columnName]
                                                def val = resultSet.getString((int) prop.columnIndex)
                                                row.createCell(columnIdx).with { HSSFCell cell ->
                                                    if (prop.isExclude) {
                                                        cell.cellStyle = prop.isTarget ? cellStyleMap.tBodyExclude : cellStyleMap.oBodyExclude
                                                    } else {
                                                        try {
                                                            if (prop.otherColumnIndex == null || val != resultSet.getString((int) prop.otherColumnIndex)) {
                                                                if (!tableWithDiffAry.contains(tableName)) {
                                                                    tableWithDiffAry.add(tableName)
                                                                }
                                                                cell.cellStyle = prop.isTarget ? cellStyleMap.tBodyAlert : cellStyleMap.oBodyAlert
                                                            } else {
                                                                cell.cellStyle = prop.isTarget ? cellStyleMap.tBody : cellStyleMap.oBody
                                                            }
                                                        } catch (SQLException ex) {
                                                            cell.cellStyle = prop.isTarget ? cellStyleMap.tBodyAlert : cellStyleMap.oBodyAlert
                                                        }
                                                    }
                                                    cell.setCellValue("${val}")
                                                }
                                            }
                                        }
                                        rowIdx++
                                    }
                                    cursorIdx++
                                    if ((cursorIdx) % 10000 == 0) logger.info("create xls: ${tableName}, cursor: ${cursorIdx}, output: ${rowIdx}${(allRowMode ? "" : ", diff mode")}")
                                }
                            }
                        } catch (IllegalArgumentException e) {
                            logger.error "Error in ${tableName}: ${e}"
                        }

                        if (rowIdx > 0) {
                            new File(XLS_OUTPUT_DIR).mkdirs()
                            def fileName = "${XLS_OUTPUT_DIR}/${XLS_PREFIX}${target}-${org}_${XLS_SUFFIX}_${tableName}.xls"
                            logger.info("Create xls file - ${fileName}")
                            new File(fileName).withOutputStream { os ->
                                write(os)
                            }
                            book.removeSheetAt(0)
                        }
                    }
                } catch (SQLSyntaxErrorException e) {
                    logger.error "Error in ${tableName}: ${e}"
                }
            }
            // sheet AllTables
            logger.info("Create new sheet - AllTables")
            book.createSheet("AllTables").with { HSSFSheet sheet ->
                // sheet AllTables, row header
                sheet.createRow(0).with { HSSFRow row ->
                    ["#", "テーブル名", "${target}", "${target}(件数)", "${org}", "${org}(件数)", "件数差異なし", "データ差異なし", "無視するカラム"].eachWithIndex { String key, int idx ->
                        row.createCell(idx).with { HSSFCell cell ->
                            cell.setCellValue(key)
                            cell.cellStyle = cellStyleMap.tHeader
                        }
                    }
                }
                allTableAry.eachWithIndex { tableName, tableIdx ->
                    def targetCount = null
                    def orgCount = null
                    if (tTableAry.contains(tableName)) {
                        targetCount = firstRow(db, 'SELECT count(*) AS count FROM ' + "${target}." + tableName).count
                    }
                    if (oTableAry.contains(tableName)) {
                        orgCount = firstRow(db, 'SELECT count(*) AS count FROM ' + "${org}." + tableName).count
                    }

                    // sheet AllTables, row body
                    sheet.createRow(tableIdx + 1).with { HSSFRow row ->
                        def excludeMsg = excludeTableColumn.containsKey(tableName) ? excludeTableColumn[tableName].join(",") : ""
                        [tableIdx + 1, tableName, tTableAry.contains(tableName), targetCount, oTableAry.contains(tableName), orgCount, targetCount == orgCount, !tableWithDiffAry.contains(tableName), excludeMsg].eachWithIndex { val, int idx ->
                            row.createCell(idx).with { HSSFCell cell ->
                                cell.cellStyle = cellStyleMap.tBody
                                if (idx == 2 && tTableAry.contains(tableName) != oTableAry.contains(tableName)) {
                                    cell.cellStyle = cellStyleMap.tBodyAlert
                                } else if (idx == 3 && targetCount != orgCount) {
                                    cell.cellStyle = cellStyleMap.tBodyAlert
                                } else if (val instanceof Boolean && !val) {
                                    cell.cellStyle = cellStyleMap.tBodyAlert
                                } else if (idx == 8) {
                                    cell.cellStyle = cellStyleMap.wrapText
                                }
                                if (val != "") {
                                    cell.setCellValue(val)
                                }
                                if (idx == 1 && linkTableNames[tableName]) {
                                    cell.cellStyle = cellStyleMap.tBodyLink
                                    HSSFHyperlink link = book.getCreationHelper().createHyperlink(HSSFHyperlink.LINK_FILE)
                                    link.setAddress("${XLS_PREFIX}${target}-${org}_${XLS_SUFFIX}_${tableName}.xls" as String)
                                    cell.setHyperlink(link)
                                }
                            }
                        }
                    }
                }
                sheet.createFreezePane(1, 1);
            }
            // sheet TableStatus
            logger.info("Create new sheet - TableStatus")
            createSheet("TableStatus").with { HSSFSheet sheet ->
                def headerColumns = ['TABLE_NAME', 'OWNER', 'TABLESPACE_NAME', 'STATUS', 'PCT_FREE',
                                     'PCT_USED', 'INITIAL_EXTENT', 'NEXT_EXTENT', 'MIN_EXTENTS', 'MAX_EXTENTS',
                                     'PCT_INCREASE', 'FREELISTS', 'FREELIST_GROUPS', 'LOGGING', 'NUM_ROWS',
                                     'BLOCKS', 'EMPTY_BLOCKS', 'AVG_ROW_LEN', 'LAST_ANALYZED',]
                def dummyMap = [:]
                headerColumns.each { dummyMap.put(it, '') }
                // sheet TableStatus, row header
                sheet.createRow(0).with { HSSFRow row ->
                    row.createCell(0).with { HSSFCell cell -> cell.cellStyle = cellStyleMap.tHeader; cell.setCellValue("#") }
                    headerColumns.eachWithIndex { String columnName, int columnIdx ->
                        row.createCell(columnIdx + 1).with { HSSFCell cell -> cell.cellStyle = cellStyleMap.tHeader; cell.setCellValue(columnName) }
                    }
                    headerColumns.eachWithIndex { String columnName, int columnIdx ->
                        row.createCell(columnIdx + headerColumns.size() + 1).with { HSSFCell cell -> cell.cellStyle = cellStyleMap.oHeader; cell.setCellValue(PREFIX + columnName) }
                    }
                }
                def tRows = rows(db, "SELECT * FROM dba_tables WHERE owner = '${target}'" as String)
                def oRows = rows(db, "SELECT * FROM dba_tables WHERE owner = '${org}'" as String)
                allTableAry.eachWithIndex { String tableName, int tableIdx ->
                    // sheet TableStatus, row body
                    sheet.createRow(tableIdx + 1).with { HSSFRow row ->
                        row.createCell(0).with { HSSFCell cell ->
                            cell.cellStyle = cellStyleMap.tBody
                            cell.setCellValue(tableIdx + 1)
                        }
                        def tmpTRows = tRows.findAll { it['TABLE_NAME'] == tableName }
                        def tmpORows = oRows.findAll { it['TABLE_NAME'] == tableName }
                        def tRow = (tmpTRows.size() == 1 ? tmpTRows[0] : dummyMap)
                        def oRow = (tmpORows.size() == 1 ? tmpORows[0] : dummyMap)
                        headerColumns.eachWithIndex { String columnName, int idx ->
                            row.createCell(idx + 1).with { HSSFCell cell ->
                                cell.cellStyle = cellStyleMap.tBody
                                cell.setCellValue(tRow[columnName])
                                if (tRow[columnName] != oRow[columnName]) {
                                    cell.cellStyle = cellStyleMap.tBodyAlert
                                }
                            }
                        }
                        headerColumns.eachWithIndex { String columnName, int idx ->
                            row.createCell(idx + headerColumns.size() + 1).with { HSSFCell cell ->
                                cell.cellStyle = cellStyleMap.oBody
                                cell.setCellValue(oRow[columnName])
                                if (tRow[columnName] != oRow[columnName]) {
                                    cell.cellStyle = cellStyleMap.oBodyAlert
                                }
                            }
                        }
                    }
                }
                sheet.createFreezePane(1, 1)
            }
            book.setSheetOrder("AllTables", 0)
            new File(XLS_OUTPUT_DIR).mkdirs()
            def fileName = "${XLS_OUTPUT_DIR}/${XLS_PREFIX}${target}-${org}_${XLS_SUFFIX}.xls"
            logger.info("Create xls file - ${fileName}")
            new File(fileName).withOutputStream { os ->
                write(os)
            }
        }
    }

    def isExclude = { String table, String column ->
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

    def tablesByOwner = { Sql sql, String owner ->
        def rtnAry = rows(sql, "SELECT TABLE_NAME FROM dba_tables WHERE owner = '${owner}'" as String).collect {
            if (includeTable.size() > 0) {
                includeTable.contains(it.table_name) ? it.table_name : []
            } else {
                it.table_name
            }
        }.flatten()
        rtnAry
    }

    def primaryKeys = { Sql sql, String table, String schema ->
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

    def rows = { Sql sql, String query ->
        logger.debug "query - " + query.replace("\n", " ").replace("\r", " ").replaceAll(" +", " ")
        sql.rows(query)
    }

    def firstRow = { Sql sql, String query ->
        logger.debug "query - " + query.replace("\n", " ").replace("\r", " ").replaceAll(" +", " ")
        sql.firstRow(query)
    }

    def createUnionQuery = { Sql sql, String tableName, String target, String org, Map replaceCols = [:] ->
        def shortNameAry = []
        def subStr = { inStr ->
            def outStr
            def shortName = inStr.length() > (30 - PREFIX.length() - 2) ?
                    inStr.substring(0, (30 - PREFIX.length() - 2)) :
                    inStr
            shortNameAry += shortName
            if (inStr == shortName) {
                outStr = shortName
            } else {
                outStr = shortName + "_${shortNameAry.count(shortName)}"
            }
            replaceCols[outStr] = inStr
            replaceCols[PREFIX.concat(outStr)] = inStr
            outStr
        }.memoize()
        def pks = primaryKeys(sql, tableName, target)
        def tCols = columns(sql, tableName, target)
        def oCols = columns(sql, tableName, org)
        logger.info "target table: ${tableName} - pks: ${pks}, cols: ${tCols}, orgCols: ${oCols}"
        def query = """SELECT COUNT(*) over() AS ${RECORD_COUNT},
${tCols.collect { "t1.${it} AS ${subStr(it)}" }.join(", ")},
${oCols.collect { "t2.${it} AS ${PREFIX}${subStr(it)}" }.join(", ")}
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
    ON ${pks.collect { "p1.${it} = t2.${it}" }.join(" AND ")}""" as String //if use limit: WHERE rownum <= $limit
        // sheet tableName
        logger.debug "query - " + query.replace("\n", " ").replace("\r", " ").replaceAll(" +", " ")
        query
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

//new groovyx.gprof.Profiler().run {
DatabaseDiff.main(args, new File(getClass().protectionDomain.codeSource.location.path).parent)
//}.prettyPrint()