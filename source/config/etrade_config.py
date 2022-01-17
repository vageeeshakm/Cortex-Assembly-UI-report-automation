from config.file_config import PATH as file_path


class PATH:
    """ PATH CONSTANT CLASS """
    JSON_FILE_PATH = file_path.REPORT_DOWNLOAD_PARAMS_PATH + "etrade/etrade.json"


class Constant:
    """ Constant class """
    DCM_SHEET = 'DCM'
    OATH_SHEET = 'Oath'
    DBM_SHEET = 'DBM'
    GEMINI_SHEET = 'Gemini'
    DATE_COLUMN_NAME = 'Date'
    DAY_COLUMN_NAME = 'Day'
    CAMPAIGN_DATE_COLUMN = 'Campaign Start Date'
    DCM_HEADER_ROWS = 21
    DCM_FOOTER_ROWS = 1
    DBM_FOOTER_ROWS = 15
    NO_ROWS_TO_SKIP = 0


class ColumnMapping:
    """ Column name and Number Mapping """
    DCM_OLA_FUNNEL = 13
    DCM_COST = 14
    DCM_DSP = 15
    DCM_DR = 16
    DCM_TARGET = 17
    DCM_TACTIC = 18
    DCM_DEVICE = 19
    DCM_MONTH = 20
    DCM_PLACEMENT_COUNT = 21
    DCM_PLACEMENT_TYPE = 22
    GEMINI_TARGET = 42
    GEMINI_MONTH = 43


class Formula:
    """ Formula class """
    DCM_COLUMN_FORMULA_DICT = {
        ColumnMapping.DCM_OLA_FUNNEL: '=M{row_number}',
        ColumnMapping.DCM_COST: '=SUMIFS(dbm.cost,dbm.date,E{row_number},dbm.create,W{row_number})+SUMIFS(oath.spend,oath.date,E{row_number},oath.create,W{row_number})+SUMIFS(am.cost,am.date,E{row_number},am.create,W{row_number})',
        ColumnMapping.DCM_DSP: '=VLOOKUP(@D:D,Key!A:C,3,0)',
        ColumnMapping.DCM_DR: '=VLOOKUP(@D:D,Key!A:B,2,0)',
        ColumnMapping.DCM_TARGET: '=VLOOKUP(@D:D,Key!A:D,4,0)',
        ColumnMapping.DCM_TACTIC: '=VLOOKUP(@D:D,Key!A:F,6,0)',
        ColumnMapping.DCM_DEVICE: '=VLOOKUP(@D:D,Key!A:G,5,0)',
        ColumnMapping.DCM_MONTH: '=MONTH(E{row_number})',
        ColumnMapping.DCM_PLACEMENT_COUNT: '=COUNTIF(D{row_number},"*MOB*")',
        ColumnMapping.DCM_PLACEMENT_TYPE: '=IFERROR(VLOOKUP(@D:D,Key!Q:R,2,0),D{row_number})'

    }

    GEMINI_COLUMN_FORMULA_DICT = {
        ColumnMapping.GEMINI_TARGET: '=VLOOKUP(@G:G,Key!N:O,2,0)',
        ColumnMapping.GEMINI_MONTH: '=MONTH(A{row_number})'
    }
