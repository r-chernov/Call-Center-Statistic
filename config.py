import os

AMO_BASE_URL = os.getenv("AMO_BASE_URL", "https://investagregatorru.amocrm.ru")
CLIENT_ID = os.getenv("CLIENT_ID", "")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "")
REDIRECT_URI = os.getenv("REDIRECT_URI", "https://investagregator73.ru/amo/callback")
AMO_DEBUG = os.getenv("AMO_DEBUG", "1") == "1"

TOKENS_FILE = os.getenv("TOKENS_FILE", "tokens.json")
USERS_MAP_FILE = os.getenv("USERS_MAP_FILE", "users_map.json")

CK_FIELD_ID = int(os.getenv("CK_FIELD_ID", "942511"))
CK_ENUM_ID = int(os.getenv("CK_ENUM_ID", "3619433"))

EXCEL_PATH = os.getenv("EXCEL_PATH", "report.xlsx")
EXCEL_SHEET = os.getenv("EXCEL_SHEET", "Sheet1")
EMPLOYEE_ID_COLUMN = os.getenv("EMPLOYEE_ID_COLUMN", "ID сотрудника")
EMPLOYEE_NAME_COLUMN = os.getenv("EMPLOYEE_NAME_COLUMN", "Сотрудник")

COL_CK_OPERATOR = os.getenv("COL_CK_OPERATOR", "ЦК ЛИД")
