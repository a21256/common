"""Chinese test fixtures for column_inference tests.

All Chinese strings used in tests are centralised here so the test
code itself stays language-neutral.  If the library ever needs to
support additional locales, add parallel resource modules.
"""

# ==================== Headers ====================

HEADER_PRODUCT_NAME = "商品名称"
HEADER_QTY = "数量"
HEADER_DATE = "日期"
HEADER_AMOUNT = "金额"
HEADER_REMARK = "备注"
HEADER_PRICE = "价格"
HEADER_PRODUCT_CODE_LONG = "产品编码信息"
HEADER_PRODUCT_CODE = "产品编码"
HEADER_CODE = "编码"
HEADER_COL_A = "A列"
HEADER_COL_B = "B列"

# ==================== Keywords ====================

KW_PRODUCT_NAME = (HEADER_PRODUCT_NAME, "名称")
KW_QTY = (HEADER_QTY,)
KW_DATE = (HEADER_DATE,)
KW_AMOUNT = (HEADER_AMOUNT,)
KW_CODE = (HEADER_CODE,)
KW_PRICE = (HEADER_PRICE,)
KW_NAME = ("名称",)

# ==================== Cell data ====================

CELL_APPLE = "苹果"
CELL_BANANA = "香蕉"
CELL_ORANGE = "橙子"
CELL_NON_NUMERIC_TEXT = "非数字文本"

# ==================== Normalize test data ====================

NORMALIZE_INPUT = "  商 品 名 称  "
NORMALIZE_EXPECTED = "商品名称"

# ==================== FieldSpec construction ====================

FIELDSPEC_KEYWORDS = (HEADER_PRICE, "Price")
