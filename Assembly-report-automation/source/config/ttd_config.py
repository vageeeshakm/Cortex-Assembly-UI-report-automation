from config.file_config import PATH as file_path


class PATH:
    """ PATH CONSTANT CLASS """
    JSON_FILE_PATH = file_path.REPORT_DOWNLOAD_PARAMS_PATH + "ttd/ttd.json"
    FTP_PARENT_DIRECTORY = '/DSP UI Reports - All Clients for Cortex - TTD - 118627/'
    FTP_PARENT_DIRECTORY_TO_UPLOAD_ASSEMBLY_AND_VARICK = "Last_14_Days_TTD_Report_{current_date}/"
    FTP_PARENT_DIRECTORY_TO_UPLOAD_SOPHIE = "Sophie Advertiser/Last_14_Days_TTD_Report_{current_date}/"
    FTP_PARENT_DIRECTORY_TO_UPLOAD_ELEVATE_REPORT = 'Elevate Geo Level Reports/Last_14_Days/{current_date}/'
    TTD_ASSEMBLY_VARICK_SCHEMA = file_path.SCHEMA_PATH + "ttd_assembly_varick_schema.json"
    TTD_ELEVATE_SCHEMA = file_path.SCHEMA_PATH + "ttd_elevate_schema.json"
    TTD_SOPHIE_SCHEMA = file_path.SCHEMA_PATH + "ttd_sophie_schema.json"


class FILENAME:
    """ FILENAME CONSTANT CLASS """
    ELEVATE_REPORT_OUTPUT_FILENAME = 'Last_14_days_Elevate_Rise_NY.xlsx'
    ASSEMBLY_REPORT_OUTPUT_FILENAME = 'Last_14_Days_{advertiser}.xlsx'
    SOPHIE_REPORT_OUTPUT_FILENAME = 'Last_14_Days_{advertiser}.xlsx'
    VARICK_REPORT_OUTPUT_FILENAME = 'Last_14_days_1800-CONTACTS, AAA, AAA Northeast, ABC Betrayal, ACU, Adam & Eve, Agnes Scott.xlsx'


class DATE:
    """ DURATION CONSTANT CLASS """
    DATE_FORMAT = "%Y%m%d"


class BUTTON:
    """ Button constant class"""
    DOWNLOAD_REPORT_XPATH = "//a[text()='Download']"


class PANDASSETTINGS:
    """ PANDASSETTINGS CONSTANT CLASS """
    ASSEMBLY_AND_VARICK_REPORT_SHEET_NAME = 'Ad Group Performance_data'
    ELEVATE_REPORT_SHEET_NAME = 'Ad Group Performance_data'
    SOPHIE_REPORT_SHEET_NAME = 'Ad Group Performance_data'
    DATE_COLUMNS_LIST_IN_SHEET = ['Date']
    COLUMNS_LIST_TO_DROP = ['Advertiser']
    ADVERTISER_COLUMN_TO_FILTER = 'Advertiser ID'


class ADVERTISERDETAILS:
    """ ADVERTISERDETAILS CONSTANT CLASS"""
    # Ad id and name mapping
    adveriser_id_name_mapping_dict_for_assembly = {
        '52ovgg2': '1800Contacts_NY',
        '4pjb0kg': '2CF_LA',
        'vq7uk46': 'AAA_DT',
        '8vn4iu2': 'Arbys_DT',
        'snvzvso': 'ASM Master Advertiser',
        '82jgotq': 'Bedrock Management_DT',
        'w3specl': 'Belkin_LA',
        '8fameyv': 'BI_Frontline_NY',
        'yzeayr1': 'BI-Combivent_NY',
        'ks77gyi': 'BI-HEARTGARD_NY',
        'yaa17aa': 'BI-Jardiance_NY',
        'lam1cld': 'BI-NexGard_NY',
        '06tfqrj': 'BI-Pradaxa_NY',
        'kykj6t8': 'BI-SpirivaAsthma_NY',
        'vyoa5h2': 'BI-SpirivaCOPD_NY',
        '5uq4b1a': 'BI-Stiolto_NY',
        '0nzwaip': 'Consumers Energy_NY',
        'eutazwj': 'Cox Automotive_DT',
        '3vpfzd0': 'CTCA_NY',
        'xo4y2a1': 'Dave & Busters',
        '4hjywqt': 'Depop',
        'ziyelby': 'DetroitZoo_DT',
        'oir3is6': 'Doner (DON#O-G9MH)_DT',
        'xqj0n6u': 'Duobrii_NY',
        'h3pk2ho': 'Elevate_Elastic_NY',
        'sb3oe2g': 'Elevate_Rise_NY',
        'usksyr6': 'Etrade_NY',
        'ashls92': 'FHE_LA',
        'pneddbb': 'FoodLion_NY',
        'ii8v7dy': 'FSL_LA',
        'vkze77m': 'FXN_LA',
        'xfp9ozp': 'GameStop',
        'eqiwlqy': 'Greater New York Hospital Association',
        'm5tipbl': 'GUM_LA',
        'ecccxb2': 'Hackensack Meridian Health',
        'w5h01l5': 'HungryHowies_LA',
        'in75or0': 'InnovAge_LA',
        'ztziym1': 'Krystal_DT',
        'ahan92g': 'LA Rams_LA',
        'dks1ggj': 'Master Advertiser',
        'qm6ny0q': 'New Advertiser',
        'p3abhv0': 'Old Dominion Freight Line_NY',
        '52kdjj1': 'OptionsHouse_NY',
        '4koiaq7': 'OwensCorning_DT',
        'jzvzpjg': 'PGATour_DT',
        'zp8om2o': 'Pulte_NY',
        'pkdtloo': 'Purell_NY',
        'xh725m1': 'RedRobin_DT',
        'uijsjtz': 'Smithfield_DT',
        'f0eflb8': 'Stronach_NY',
        'pya3ykt': 'Test Advertiser',
        't04dh8g': 'The Healthcare Education Project (SKDK) New York State',
        'q093cwl': 'TouchstoneEnergy_NY',
        '5v4bpcu': 'Transamerica_LA',
        'tsuuxw1': 'Truth_NY',
        'f98xkp8': 'TUPSS_DT',
        '9xe2csl': 'TUPSSCo-op_LA',
        'l8hfnf9': 'Wordpress_NY'
    }
    adveriser_id_name_mapping_dict_for_sophie = {
        'rt8plu8': 'MD Anderson',
    }
    ELEVATE_ADVERTISER_NAME_TYPE = 'Elevate_rise'
    MEDIA_ASSEMBLY_ADVERTISER_TYPE = 'Media_assembly'
    VARICK_ADVERTISER_TYPE = 'Varick_media'
    SOPHIE_ADVERTISER_TYPE = 'Sophie'
