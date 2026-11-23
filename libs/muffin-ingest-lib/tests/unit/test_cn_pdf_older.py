"""The CNINFO reader's older forms (FY2007-2014), read from positions captured off the reports.

Each fixture is a page's items between two heights, as `page_items` gives them. The current forms
read none of these tables, which is why `older` exists; it runs only when they give nothing.
"""

from __future__ import annotations

import pytest

from muffin_ingest.facets import cn_pdf
from muffin_ingest.facets.cn_pdf import TextItem

#: 大同煤业 (601001) FY2010, page 20: one table for industry and product, its `分行业` and `分产品`
#: rows above each group, a note that it lists only the lines at or above 10%, and the region table
#: with no cost column.
DATONG = [
    TextItem("(1) 主营业务分行业、产品情况", 100.8, 688.4, 247.8),
    TextItem("单位:元 币种:人民币", 384.2, 676.4, 484.0),
    TextItem("营 业 收", 354.2, 662.9, 394.7),
    TextItem("分 行 业 或", 85.1, 650.9, 137.3),
    TextItem("营 业 利", 315.1, 650.9, 352.7),
    TextItem("入 比 上", 354.2, 650.9, 394.7),
    TextItem("营 业 成 本 比", 396.2, 650.9, 455.6),
    TextItem("营 业 利 润率比", 457.0, 650.9, 525.2),
    TextItem("营业收入", 138.8, 644.9, 180.8),
    TextItem("营业成本", 229.6, 644.9, 271.6),
    TextItem("分产品", 85.1, 638.9, 116.6),
    TextItem("润率(%)", 315.1, 638.9, 351.8),
    TextItem("年 增 减", 354.2, 638.9, 394.7),
    TextItem("上年增减(%)", 396.2, 638.9, 453.9),
    TextItem("上年增减（%）", 457.0, 638.9, 525.2),
    TextItem("(%)", 354.2, 626.9, 369.9),
    TextItem("分行业", 85.1, 613.3, 116.6),
    TextItem("减少 5.28 个百", 457.0, 599.8, 525.2),
    TextItem("煤炭行业", 85.1, 593.8, 127.1),
    TextItem("10,290,002,348.07 5,525,550,658.39", 138.9, 593.8, 313.5),
    TextItem("46.30", 326.4, 593.8, 352.6),
    TextItem("9.29", 373.6, 593.8, 394.6),
    TextItem("21.21 ", 429.2, 593.8, 455.5),
    TextItem("分点", 504.2, 587.8, 525.2),
    TextItem("分产品", 85.1, 574.3, 116.6),
    TextItem("减少 5.28 个百", 457.0, 560.8, 525.2),
    TextItem("煤炭", 85.1, 554.8, 106.1),
    TextItem("10,290,002,348.07 5,525,550,658.39", 138.9, 554.8, 313.5),
    TextItem("46.30", 326.4, 554.8, 352.6),
    TextItem("9.29", 373.6, 554.8, 394.6),
    TextItem("21.21 ", 429.2, 554.8, 455.5),
    TextItem("分点", 504.2, 548.8, 525.2),
    TextItem(
        "注：分别按照行业、产品列示占营业收入或营业利润 10%以上的主要行业和产品。",
        85.1,
        535.4,
        460.5,
    ),
    TextItem(
        "说明：1、煤炭总产量：全公司生产煤炭 2929 万吨，其中：母公司煤炭产量 849 万吨；塔山",
        85.1,
        523.4,
        510.3,
    ),
    TextItem("煤矿煤炭 2080 万吨。", 85.1, 511.4, 184.8),
    TextItem("2、", 182.2, 511.4, 198.0),
    TextItem("总进尺：", 195.4, 511.4, 237.3),
    TextItem("全公司累计总进尺 77538 米，", 234.7, 511.4, 371.2),
    TextItem("其中：", 368.6, 511.4, 400.0),
    TextItem("母公司累计总进尺 56738", 397.4, 511.4, 510.3),
    TextItem(
        "米；塔山煤矿累计总进尺 20800 米。3、入洗煤情况：全公司累计入洗原煤 1687 万吨，洗出",
        85.1,
        499.4,
        510.3,
    ),
    TextItem(
        "煤量 1027 万吨，累计洗煤回收率 60.88%；折算洗耗率为 39.12%。4、煤炭总销量：全公司煤",
        85.1,
        487.4,
        510.2,
    ),
    TextItem(
        "炭销售 2252 万吨，其中：母公司销量 839 万吨；塔山煤矿销量 1413 万吨。母公司煤炭销售",
        85.1,
        475.4,
        510.3,
    ),
    TextItem(
        "构成：外销 740 万吨，其中洗煤 3 万吨；地销 99 万吨。塔山煤矿煤炭销售构成：外销洗精煤",
        85.1,
        463.4,
        510.3,
    ),
    TextItem("899 万吨，", 85.1, 451.4, 135.0),
    TextItem("地销塔山电厂原煤 382 万吨，", 132.4, 451.4, 268.8),
    TextItem("地销大唐热电原煤和中煤 129 万吨，", 266.2, 451.4, 434.2),
    TextItem("其他地销 3 万吨。", 431.5, 451.4, 515.5),
    TextItem(
        "5、金宇高岭土化工有限公司高岭岩加工累计完成 18815 吨，累计销售 17503 吨。6、同塔建",
        85.1,
        439.4,
        510.3,
    ),
    TextItem("材公司累计生产烧结砖 1681 万块，累计销售 655 万块。", 85.1, 427.4, 342.3),
    TextItem("(2)主营业务分地区情况", 106.1, 403.4, 216.3),
    TextItem("单位:元 币种:人民币", 400.0, 391.4, 499.7),
    TextItem("地区", 85.1, 377.9, 106.1),
    TextItem("营业收入", 206.1, 377.9, 248.1),
    TextItem("营业收入比上年增减（％）", 357.5, 377.9, 483.5),
    TextItem("国内", 85.1, 364.3, 106.1),
    TextItem("10,322,197,979.55", 206.2, 364.3, 295.4),
    TextItem("9.56", 357.6, 364.3, 378.6),
]

#: 太钢不锈 (000825) FY2010, page 27: `主营业务收入`/`主营业务成本` labels, the name label set as
#: `分行业或分` over `产品`, a product table under its own heading but no header of its own, then a
#: share table (`主营业务收入 | 主营业务利润`, `按行业`, `按产品`).
TAIGANG = [
    TextItem("（", 109.1, 735.2, 121.1),
    TextItem("1", 121.1, 735.2, 127.1),
    TextItem("）主营业务分行业、产品、主营业务利润情况：", 127.1, 735.2, 379.1),
    TextItem(" 单位：元", 468.3, 720.2, 510.5),
    TextItem("主营业务收入分行业情况", 248.1, 707.0, 347.2),
    TextItem("主营业务", 361.9, 693.5, 397.9),
    TextItem("主营业务", 414.2, 693.5, 450.2),
    TextItem("主营业务", 307.2, 687.7, 343.2),
    TextItem("主营业务利", 464.1, 687.7, 509.1),
    TextItem("分行业或分", 85.5, 681.8, 130.5),
    TextItem("收入比上", 361.9, 681.8, 397.9),
    TextItem("成本比上", 414.2, 681.8, 450.2),
    TextItem("主营业务收入", 149.5, 675.9, 203.5),
    TextItem("主营业务成本", 230.3, 675.9, 284.3),
    TextItem("利润率", 311.8, 675.9, 338.8),
    TextItem("润率比上年", 464.1, 675.9, 509.1),
    TextItem("产品", 98.9, 670.1, 116.9),
    TextItem("年增减", 366.3, 670.1, 393.3),
    TextItem("年增减", 418.8, 670.1, 445.8),
    TextItem("（％）", 311.8, 664.3, 338.8),
    TextItem("增减（％）", 464.1, 664.3, 509.1),
    TextItem("（％）", 366.3, 658.4, 393.3),
    TextItem("（％）", 418.8, 658.4, 445.8),
    TextItem("冶金钢铁", 89.9, 646.2, 125.9),
    TextItem("87,108,880,561.31", 144.0, 640.5, 211.6),
    TextItem("79,389,141,359.20", 224.8, 640.5, 292.3),
    TextItem("8.86", 331.9, 640.5, 347.6),
    TextItem("22.55", 381.1, 640.5, 401.4),
    TextItem("22.85", 432.0, 640.5, 452.3),
    TextItem("-0.22", 491.6, 640.5, 510.4),
    TextItem("行业", 98.9, 634.5, 116.9),
    TextItem("主营业务分产品情况", 257.1, 620.0, 338.2),
    TextItem("37,794,443,757.22", 144.0, 603.5, 211.6),
    TextItem("34,094,367,713.38", 224.8, 603.5, 292.4),
    TextItem("9.79", 331.9, 603.5, 347.6),
    TextItem("35.98", 381.1, 603.5, 401.4),
    TextItem("34.23", 432.0, 603.5, 452.3),
    TextItem("1.18", 494.6, 603.5, 510.4),
    TextItem("不锈钢材", 89.9, 603.3, 125.9),
    TextItem("24,567,945,269.89", 144.0, 586.8, 211.6),
    TextItem("20,924,518,986.37", 224.8, 586.8, 292.4),
    TextItem("14.83", 327.3, 586.8, 347.6),
    TextItem("21.96", 381.1, 586.8, 401.4),
    TextItem("25.24", 432.0, 586.8, 452.3),
    TextItem("-2.23", 491.6, 586.8, 510.4),
    TextItem("普通钢材", 89.9, 586.7, 125.9),
    TextItem("3,932,562,801.69", 148.6, 570.1, 211.6),
    TextItem("3,829,136,400.01", 229.4, 570.1, 292.4),
    TextItem("2.63", 331.9, 570.1, 347.6),
    TextItem("28.67", 381.1, 570.1, 401.4),
    TextItem("32.09", 432.0, 570.1, 452.3),
    TextItem("-2.52", 491.6, 570.1, 510.4),
    TextItem("普通钢坯", 89.9, 570.0, 125.9),
    TextItem("20,813,928,732.51", 144.0, 553.4, 211.6),
    TextItem("20,541,118,259.44", 225.2, 553.4, 292.4),
    TextItem("1.31", 331.9, 553.4, 347.6),
    TextItem("3.63", 385.6, 553.4, 401.4),
    TextItem("4.70", 436.5, 553.4, 452.3),
    TextItem("-1.01", 491.6, 553.4, 510.4),
    TextItem("其他", 98.9, 553.3, 116.9),
    TextItem("主营业务收入", 238.0, 524.6, 292.0),
    TextItem("主营业务利润", 404.7, 524.6, 458.7),
    TextItem("划分标准", 112.9, 512.6, 148.9),
    TextItem("比例", 275.3, 510.4, 293.3),
    TextItem("同比增", 312.8, 510.4, 339.8),
    TextItem("比例", 434.5, 510.4, 452.5),
    TextItem("同比增减", 471.5, 510.4, 507.5),
    TextItem("金额", 214.1, 504.5, 232.1),
    TextItem("金额", 376.8, 504.5, 394.8),
    TextItem("（", 270.9, 498.7, 279.9),
    TextItem("%", 280.0, 498.7, 289.0),
    TextItem("）", 288.8, 498.7, 297.8),
    TextItem("减（", 310.1, 498.7, 328.1),
    TextItem("%", 328.3, 498.7, 337.3),
    TextItem("）", 337.0, 498.7, 346.0),
    TextItem("（", 429.9, 498.7, 438.9),
    TextItem("%", 439.0, 498.7, 448.0),
    TextItem("）", 447.8, 498.7, 456.8),
    TextItem("（", 476.0, 498.7, 485.0),
    TextItem("%", 485.1, 498.7, 494.1),
    TextItem("）", 493.9, 498.7, 502.9),
    TextItem("冶金钢铁", 136.0, 486.4, 172.0),
    TextItem("87,108,880,561.31", 191.1, 480.8, 258.6),
    TextItem("100.00", 274.6, 480.8, 299.4),
    TextItem("22.55", 322.1, 480.8, 342.5),
    TextItem("7,719,739,202.11", 355.6, 480.8, 418.3),
    TextItem("100.00", 433.1, 480.8, 457.9),
    TextItem("19.61", 490.1, 480.8, 510.4),
    TextItem("按行业", 89.2, 480.7, 116.2),
    TextItem("行业", 145.0, 474.8, 163.0),
    TextItem("37,794,443,757.22", 191.1, 460.8, 258.6),
    TextItem("43.39", 279.0, 460.8, 299.4),
    TextItem("35.98", 322.1, 460.8, 342.5),
    TextItem("3,700,076,043.84", 355.3, 460.8, 418.3),
    TextItem("47.93", 437.6, 460.8, 457.9),
    TextItem("54.62", 490.1, 460.8, 510.4),
    TextItem("不锈钢材", 136.0, 460.6, 172.0),
    TextItem("24,567,945,269.89", 191.1, 444.5, 258.6),
    TextItem("28.20", 279.0, 444.5, 299.4),
    TextItem("21.96", 322.1, 444.5, 342.5),
    TextItem("3,643,426,283.52", 355.3, 444.5, 418.3),
    TextItem("47.20", 437.6, 444.5, 457.9),
    TextItem("6.01", 494.6, 444.5, 510.4),
    TextItem("普通钢材", 136.0, 444.4, 172.0),
    TextItem("按产品", 89.2, 436.3, 116.2),
    TextItem("3,932,562,801.69", 195.6, 428.4, 258.6),
    TextItem("4.52", 283.6, 428.4, 299.4),
    TextItem("28.67", 322.1, 428.4, 342.5),
    TextItem("103,426,401.68", 362.0, 428.4, 418.3),
    TextItem("1.34", 442.2, 428.4, 457.9),
    TextItem("-34.29", 487.1, 428.4, 510.4),
    TextItem("普通钢坯", 136.0, 428.2, 172.0),
]

#: 永新股份 (002014) FY2010, page 29: a table per axis, headed `分行业经营情况（单位：人民币万元）`,
#: the growth columns' labels a line above the header, and a memo line under each table.
YONGXIN = [
    TextItem("1", 109.1, 497.6, 115.1),
    TextItem("、分行业经营情况", 115.1, 497.6, 211.1),
    TextItem("（单位：人民币万元）", 211.1, 497.6, 316.4),
    TextItem("营业收入", 352.1, 477.1, 394.4),
    TextItem("营业成本", 405.4, 477.1, 447.7),
    TextItem("营业利润", 463.4, 477.1, 505.7),
    TextItem("毛利率", 308.5, 469.3, 340.1),
    TextItem("分行业", 111.5, 461.5, 143.2),
    TextItem("营业收入", 185.7, 461.5, 227.9),
    TextItem("营业成本", 249.2, 461.5, 291.4),
    TextItem("比上年增", 352.1, 461.5, 394.4),
    TextItem("比上年增", 405.4, 461.5, 447.7),
    TextItem("率比上年", 463.4, 461.5, 505.7),
    TextItem("（", 308.6, 453.7, 319.1),
    TextItem("%", 319.1, 453.7, 329.7),
    TextItem("）", 329.6, 453.7, 340.1),
    TextItem("减（", 352.1, 445.9, 373.3),
    TextItem("%", 373.3, 445.9, 383.8),
    TextItem("）", 383.7, 445.9, 394.3),
    TextItem("减（", 405.4, 445.9, 426.5),
    TextItem("%", 426.5, 445.9, 437.1),
    TextItem("）", 437.0, 445.9, 447.5),
    TextItem("增减（", 458.6, 445.9, 490.3),
    TextItem("%", 490.3, 445.9, 500.9),
    TextItem("）", 500.7, 445.9, 511.3),
    TextItem("下降", 460.8, 427.1, 478.8),
    TextItem("3.51", 481.1, 427.1, 496.8),
    TextItem("个", 499.2, 427.1, 508.2),
    TextItem("124,645.94", 180.6, 418.8, 228.0),
    TextItem("101,308.42", 244.1, 418.8, 291.5),
    TextItem("18.72", 312.4, 418.8, 336.2),
    TextItem("19.81", 365.6, 418.8, 389.4),
    TextItem("25.21", 418.9, 418.8, 442.6),
    TextItem("其它塑料制造业", 85.1, 418.6, 159.0),
    TextItem("百分点", 471.0, 411.5, 498.0),
    TextItem("0", 342.8, 391.1, 348.1),
    TextItem("其中：关联交易", 85.1, 391.0, 159.0),
    TextItem("2", 109.1, 367.5, 115.1),
    TextItem("、分产品经营情况", 115.1, 367.5, 211.1),
    TextItem("（单位：人民币万元）", 211.1, 367.5, 316.4),
    TextItem("营业收入", 351.1, 344.6, 393.3),
    TextItem("营业成本", 404.8, 344.6, 447.1),
    TextItem("营业利润", 463.2, 344.6, 505.4),
    TextItem("毛利率", 307.0, 336.8, 338.7),
    TextItem("分产品", 113.7, 329.0, 145.3),
    TextItem("营业收入", 189.9, 329.0, 232.1),
    TextItem("营业成本", 250.4, 329.0, 292.6),
    TextItem("比上年增", 351.1, 329.0, 393.3),
    TextItem("比上年增", 404.8, 329.0, 447.1),
    TextItem("率比上年", 463.2, 329.0, 505.4),
    TextItem("（", 307.1, 321.2, 317.7),
    TextItem("%", 317.7, 321.2, 328.3),
    TextItem("）", 328.1, 321.2, 338.7),
    TextItem("减（", 351.1, 313.4, 372.2),
    TextItem("%", 372.2, 313.4, 382.8),
    TextItem("）", 382.6, 313.4, 393.2),
    TextItem("减（", 404.8, 313.4, 425.9),
    TextItem("%", 426.0, 313.4, 436.5),
    TextItem("）", 436.4, 313.4, 447.0),
    TextItem("增减（", 458.3, 313.4, 489.9),
    TextItem("%", 489.9, 313.4, 500.5),
    TextItem("）", 500.4, 313.4, 510.9),
    TextItem("下降", 460.7, 292.2, 478.7),
    TextItem("3.96", 480.9, 292.2, 496.7),
    TextItem("个", 499.1, 292.2, 508.1),
    TextItem("117,209.18", 184.8, 283.9, 231.9),
    TextItem("94,447.99", 247.9, 283.9, 290.0),
    TextItem("19.42", 311.0, 283.9, 334.7),
    TextItem("19.61", 364.9, 283.9, 388.6),
    TextItem("25.78", 418.6, 283.9, 442.4),
    TextItem("彩印复合包装材料", 85.1, 283.7, 169.6),
    TextItem("百分点", 470.7, 276.6, 497.7),
    TextItem("上升", 460.7, 259.0, 478.7),
    TextItem("4.56", 480.9, 259.0, 496.7),
    TextItem("个", 499.1, 259.0, 508.1),
    TextItem("4,544.21", 195.1, 250.6, 231.9),
    TextItem("4,205.10", 253.1, 250.6, 290.0),
    TextItem("7.46", 316.3, 250.6, 334.8),
    TextItem("-2.35", 366.7, 250.6, 388.5),
    TextItem("-6.94", 420.4, 250.6, 442.3),
    TextItem("真空镀铝包装材料", 85.1, 250.5, 169.6),
    TextItem("百分点", 470.7, 243.4, 497.7),
    TextItem("上升", 460.7, 227.3, 478.7),
    TextItem("2.34", 480.9, 227.3, 496.7),
    TextItem("个", 499.1, 227.3, 508.1),
    TextItem("2,892.55", 195.1, 218.9, 231.9),
    TextItem("2,655.33", 253.1, 218.9, 290.0),
    TextItem("8.20", 316.3, 218.9, 334.8),
    TextItem("108.48", 359.6, 218.9, 388.6),
    TextItem("103.30", 413.4, 218.9, 442.4),
    TextItem("塑料软包装薄膜", 85.1, 218.8, 159.0),
    TextItem("百分点", 470.7, 211.7, 497.7),
    TextItem("0", 345.0, 192.9, 350.2),
    TextItem("其中：关联交易", 85.1, 192.7, 159.0),
]

#: The borders 永新股份's page draws across the name column.
YONGXIN_RULES = [
    cn_pdf.Rule(489.4, 80.4, 174.7),
    cn_pdf.Rule(488.6, 80.4, 174.7),
    cn_pdf.Rule(442.0, 79.7, 175.0),
    cn_pdf.Rule(441.6, 80.4, 174.7),
    cn_pdf.Rule(440.1, 80.4, 174.7),
    cn_pdf.Rule(439.7, 80.4, 174.7),
    cn_pdf.Rule(385.0, 79.7, 175.0),
    cn_pdf.Rule(384.2, 80.4, 174.7),
    cn_pdf.Rule(359.3, 80.4, 179.2),
    cn_pdf.Rule(358.5, 80.4, 179.2),
    cn_pdf.Rule(307.1, 79.7, 179.4),
    cn_pdf.Rule(306.7, 80.4, 179.2),
    cn_pdf.Rule(305.2, 80.4, 179.2),
    cn_pdf.Rule(304.8, 80.4, 179.2),
    cn_pdf.Rule(186.7, 79.7, 179.4),
    cn_pdf.Rule(185.9, 80.4, 179.2),
]

#: 凤凰光学 (600071) FY2010, page 18: the name label set as `分行业或分产` over `品`, a `小计`, a
#: positive `合并抵消` and a `合计` in each group.
FENGHUANG = [
    TextItem(" ①主营业务分行业、分产品情况表", 78.6, 665.7, 236.0),
    TextItem(" 单位", 377.7, 652.1, 398.7),
    TextItem(":", 398.7, 652.1, 401.6),
    TextItem("元 币种", 401.6, 652.1, 438.3),
    TextItem(":", 438.3, 652.1, 441.2),
    TextItem("人民币", 441.2, 652.1, 472.7),
    TextItem(" 营业收入", 360.3, 638.4, 402.3),
    TextItem(" 营业成本", 411.3, 638.4, 453.3),
    TextItem("分行业或分产", 52.8, 632.4, 115.8),
    TextItem("营业利润", 309.2, 632.4, 351.2),
    TextItem(" 营业利润率比上年增", 459.0, 632.3, 553.5),
    TextItem("营业收入", 144.2, 626.4, 186.2),
    TextItem("营业成本", 237.2, 626.4, 279.2),
    TextItem("比上年增", 360.3, 626.3, 402.3),
    TextItem("比上年增", 411.3, 626.3, 453.3),
    TextItem("品", 79.1, 620.4, 89.6),
    TextItem("率", 317.1, 620.4, 327.6),
    TextItem("(%)", 327.6, 620.4, 343.3),
    TextItem("减（", 486.1, 620.3, 507.1),
    TextItem("%", 507.1, 620.3, 515.8),
    TextItem("）", 515.8, 620.3, 526.3),
    TextItem("减", 368.2, 614.3, 378.7),
    TextItem("(%)", 378.7, 614.3, 394.4),
    TextItem("减", 419.2, 614.3, 429.7),
    TextItem("(%)", 429.7, 614.3, 445.5),
    TextItem("分行业", 50.8, 600.9, 82.3),
    TextItem("制造业", 50.8, 587.4, 82.3),
    TextItem(" 1,399,725,575.33", 137.4, 587.4, 210.9),
    TextItem("1,223,027,629.06", 230.3, 587.4, 303.9),
    TextItem("12.62", 331.3, 587.4, 355.0),
    TextItem("46.47", 382.4, 587.4, 406.1),
    TextItem("49.13", 433.5, 587.4, 457.2),
    TextItem(" 减少", 467.2, 587.4, 488.2),
    TextItem("1.56", 490.8, 587.4, 509.2),
    TextItem("个百分点", 511.9, 587.4, 553.8),
    TextItem("商业", 50.8, 573.9, 71.8),
    TextItem(" 46,655,619.47", 150.6, 573.9, 211.0),
    TextItem("45,023,359.39", 243.5, 573.9, 303.9),
    TextItem("3.50", 336.6, 573.9, 355.0),
    TextItem("70.31", 382.4, 573.9, 406.1),
    TextItem("79.81", 433.5, 573.9, 457.2),
    TextItem(" 减少", 467.2, 573.9, 488.2),
    TextItem("5.10", 490.8, 573.9, 509.2),
    TextItem("个百分点", 511.9, 573.9, 553.8),
    TextItem("小计", 50.8, 560.4, 71.8),
    TextItem(" 1,446,381,194.80", 137.4, 560.4, 210.9),
    TextItem("1,268,050,988.45", 230.3, 560.4, 303.9),
    TextItem("12.33", 331.3, 560.4, 355.0),
    TextItem("47.14", 382.4, 560.4, 406.1),
    TextItem("50.04", 433.5, 560.4, 457.2),
    TextItem(" 减少", 467.2, 560.4, 488.2),
    TextItem("1.69", 490.8, 560.4, 509.2),
    TextItem("个百分点", 511.9, 560.4, 553.8),
    TextItem("合并抵消", 50.8, 546.9, 92.8),
    TextItem(" 83,800,905.02", 150.6, 546.9, 211.0),
    TextItem("90,368,154.60", 243.5, 546.9, 303.9),
    TextItem("合计", 50.8, 533.4, 71.8),
    TextItem(" 1,362,580,289.78", 137.4, 533.4, 210.9),
    TextItem("1,177,682,833.85", 230.3, 533.4, 303.9),
    TextItem("13.57", 331.3, 533.4, 355.0),
    TextItem("55.21", 382.4, 533.4, 406.1),
    TextItem("60.44", 433.5, 533.4, 457.2),
    TextItem(" 减少", 467.2, 533.4, 488.2),
    TextItem("2.82", 490.8, 533.4, 509.2),
    TextItem("个百分点", 511.9, 533.4, 553.8),
    TextItem("分产品", 50.8, 519.9, 82.3),
    TextItem("光学加工", 50.8, 506.4, 92.8),
    TextItem(" 1,253,566,488.13", 137.4, 506.4, 210.9),
    TextItem("1,096,585,345.77", 230.3, 506.4, 303.9),
    TextItem("12.52", 331.3, 506.4, 355.0),
    TextItem("57.08", 382.4, 506.4, 406.1),
    TextItem("60.39", 433.5, 506.4, 457.2),
    TextItem(" 减少", 467.2, 506.4, 488.2),
    TextItem("1.81", 490.8, 506.4, 509.2),
    TextItem("个百分点", 511.9, 506.4, 553.8),
    TextItem("精密加工", 50.8, 492.9, 92.8),
    TextItem(" 101,185,645.40", 145.3, 492.9, 210.9),
    TextItem("87,455,467.33", 243.5, 492.9, 303.9),
    TextItem("13.57", 331.3, 492.9, 355.0),
    TextItem("17.76", 382.4, 492.9, 406.1),
    TextItem("15.60", 433.5, 492.9, 457.2),
    TextItem(" 增加", 467.2, 492.9, 488.2),
    TextItem("1.62", 490.8, 492.9, 509.2),
    TextItem("个百分点", 511.9, 492.9, 553.8),
    TextItem("照相器材", 50.8, 479.4, 92.8),
    TextItem(" 91,629,061.27", 150.6, 479.4, 211.0),
    TextItem("84,010,175.35", 243.5, 479.4, 303.9),
    TextItem("8.31", 336.6, 479.4, 355.0),
    TextItem("-7.50", 384.2, 479.4, 406.2),
    TextItem("-2.11", 435.7, 479.4, 457.2),
    TextItem(" 减少", 467.2, 479.4, 488.2),
    TextItem("5.05", 490.8, 479.4, 509.2),
    TextItem("个百分点", 511.9, 479.4, 553.8),
    TextItem("小计", 50.8, 465.9, 71.8),
    TextItem(" 1,446,381,194.80", 137.4, 465.9, 210.9),
    TextItem("1,268,050,988.45", 230.3, 465.9, 303.9),
    TextItem("12.33", 331.3, 465.9, 355.0),
    TextItem("47.14", 382.4, 465.9, 406.1),
    TextItem("50.04", 433.5, 465.9, 457.2),
    TextItem(" 减少", 467.2, 465.9, 488.2),
    TextItem("1.69", 490.8, 465.9, 509.2),
    TextItem("个百分点", 511.9, 465.9, 553.8),
    TextItem("合并抵消", 50.8, 452.4, 92.8),
    TextItem(" 83,800,905.02", 150.6, 452.4, 211.0),
    TextItem("90,368,154.60", 243.5, 452.4, 303.9),
    TextItem("合计", 50.8, 438.9, 71.8),
    TextItem(" 1,362,580,289.78", 137.4, 438.9, 210.9),
    TextItem("1,177,682,833.85", 230.3, 438.9, 303.9),
    TextItem("13.57", 331.3, 438.9, 355.0),
    TextItem("55.21", 382.4, 438.9, 406.1),
    TextItem("60.44", 433.5, 438.9, 457.2),
    TextItem(" 减少", 467.2, 438.9, 488.2),
    TextItem("2.82", 490.8, 438.9, 509.2),
    TextItem("个百分点", 511.9, 438.9, 553.8),
]

#: 振华重工 (600320) FY2010, page 24: whole numbers in a table stated in yuan.
ZPMC = [
    TextItem("①主营业务分行业、分产品情况表", 68.4, 492.4, 226.4),
    TextItem("单位:元 币种:人民币", 427.5, 479.4, 513.0),
    TextItem("主营业", 313.1, 465.2, 340.2),
    TextItem("主营业务", 354.6, 465.2, 390.7),
    TextItem("主营业务成", 406.2, 465.2, 451.3),
    TextItem("分行业或", 89.9, 457.5, 126.1),
    TextItem("主营业务利润率比", 467.9, 457.4, 540.2),
    TextItem("主营业务收入", 158.2, 449.6, 212.4),
    TextItem("主营业务成本", 238.3, 449.6, 292.4),
    TextItem("务利润", 313.1, 449.6, 340.2),
    TextItem("收入比上", 354.6, 449.6, 390.7),
    TextItem("本比上年增", 406.2, 449.6, 451.3),
    TextItem("分产品", 94.4, 443.8, 121.6),
    TextItem("上年增减(%)", 479.2, 441.8, 528.9),
    TextItem("率(%)", 315.3, 434.0, 337.9),
    TextItem("年增减(%)", 352.3, 434.0, 392.9),
    TextItem("减(%)", 417.4, 434.0, 440.0),
    TextItem("集装箱起重机", 74.0, 417.9, 128.0),
    TextItem("8,576,009,665", 159.3, 417.9, 217.8),
    TextItem("8,496,041,195", 243.5, 417.9, 302.0),
    TextItem("0.93", 322.6, 417.9, 340.6),
    TextItem("-49.21", 367.0, 417.9, 394.0),
    TextItem("-45.13", 425.8, 417.9, 452.8),
    TextItem("减少 7.37 个百分点", 468.1, 417.9, 544.6),
    TextItem("散货机械", 74.0, 401.8, 110.0),
    TextItem("3,690,151,188", 159.3, 401.8, 217.8),
    TextItem("3,834,464,868", 243.5, 401.8, 302.0),
    TextItem("-3.91", 318.1, 401.8, 340.6),
    TextItem("18.59", 371.5, 401.8, 394.0),
    TextItem("25.98", 430.3, 401.8, 452.8),
    TextItem("减少 6.10 个百分点", 468.1, 401.8, 544.6),
    TextItem("海上重型装备", 74.0, 385.7, 128.0),
    TextItem("2,591,343,260", 159.3, 385.7, 217.8),
    TextItem("2,339,329,475", 243.5, 385.7, 302.0),
    TextItem("9.73", 322.6, 385.7, 340.6),
    TextItem("-28.70", 367.0, 385.7, 394.0),
    TextItem("-5.54", 430.3, 385.7, 452.8),
    TextItem("减少 22.13 个百分点", 463.6, 385.7, 544.6),
    TextItem("钢结构", 74.0, 369.6, 101.0),
    TextItem("1,662,991,694", 159.3, 369.6, 217.8),
    TextItem("1,442,401,497", 243.5, 369.6, 302.0),
    TextItem("13.26", 318.1, 369.6, 340.6),
    TextItem("-43.67", 367.0, 369.6, 394.0),
    TextItem("-45.58", 425.8, 369.6, 452.8),
    TextItem("增加 3.04 个百分点", 468.1, 369.6, 544.6),
    TextItem("船舶运输及其他", 74.0, 353.5, 137.0),
    TextItem("188,650,160", 168.3, 353.5, 217.8),
    TextItem("115,932,839", 252.5, 353.5, 302.0),
    TextItem("38.55", 318.1, 353.5, 340.6),
    TextItem("440.35", 367.0, 353.5, 394.0),
    TextItem("396.79", 425.8, 353.5, 452.8),
    TextItem("增加 5.39 个百分点", 468.1, 353.5, 544.6),
]


def rows(
    items: list[TextItem], *, older: bool = True
) -> list[tuple[str, str, float, float | None]]:
    return [
        (r.axis, r.name, r.revenue, r.cost)
        for r in cn_pdf.parse_segment_page(items, [], older=older)
        if not r.stated_total
    ]


@pytest.mark.parametrize("items", [DATONG, TAIGANG, YONGXIN, FENGHUANG, ZPMC])
def test_the_current_forms_read_nothing_of_an_older_table(items: list[TextItem]) -> None:
    assert cn_pdf.parse_segment_page(items, []) == []


def test_the_combined_table_splits_by_its_group_rows_and_the_region_table_has_no_cost() -> None:
    assert rows(DATONG) == [
        ("cninfo:分行业", "煤炭行业", 10290002348.07, 5525550658.39),
        ("cninfo:分产品", "煤炭", 10290002348.07, 5525550658.39),
        ("cninfo:分地区", "国内", 10322197979.55, None),
    ]


def test_partial_lists_are_reconciled_against_the_largest_table() -> None:
    """The industry and product tables list one line each, the lines at or above 10%, and agree
    with each other; the region table is the whole. The total most tables agree on would serve
    the two lists as whole splits."""
    found = cn_pdf.parse_segment_page(DATONG, [], older=True)
    placed = {
        (f.axis, f.partition_id > 0, f.reconciled_to)
        for f in cn_pdf.facts_from_rows(found, "2010-12-31", partial_lists=True)
        if f.metric_code == "revenue"
    }
    assert placed == {
        ("cninfo:分行业", False, 10322197979.55),
        ("cninfo:分产品", False, 10322197979.55),
        ("cninfo:分地区", True, 10322197979.55),
    }
    agreed = {
        (f.axis, f.partition_id > 0)
        for f in cn_pdf.facts_from_rows(found, "2010-12-31")
        if f.metric_code == "revenue"
    }
    assert ("cninfo:分行业", True) in agreed, "the current rule picks the lists"


def test_a_product_table_with_no_header_of_its_own_uses_the_industry_tables_columns() -> None:
    read = rows(TAIGANG)
    assert read[0] == ("cninfo:分行业", "冶金钢铁行业", 87108880561.31, 79389141359.20)
    assert [r for r in read if r[0] == "cninfo:分产品"] == [
        ("cninfo:分产品", "不锈钢材", 37794443757.22, 34094367713.38),
        ("cninfo:分产品", "普通钢材", 24567945269.89, 20924518986.37),
        ("cninfo:分产品", "普通钢坯", 3932562801.69, 3829136400.01),
        ("cninfo:分产品", "其他", 20813928732.51, 20541118259.44),
    ], "the share table after it is not read"
    assert len(read) == 5


def test_separate_tables_headed_with_their_unit_and_a_memo_under_each() -> None:
    assert rows(YONGXIN) == [
        ("cninfo:分行业", "其它塑料制造业", 1246459400.0, 1013084200.0),
        ("cninfo:分产品", "彩印复合包装材料", 1172091800.0, 944479900.0),
        ("cninfo:分产品", "真空镀铝包装材料", 45442100.0, 42051000.0),
        ("cninfo:分产品", "塑料软包装薄膜", 28925500.0, 26553300.0),
    ]


def test_a_memo_under_a_row_joins_no_member_name() -> None:
    """Read with the page's borders, which place `其中：关联交易` in the row's cell."""
    read = [
        (r.axis, r.name)
        for r in cn_pdf.parse_segment_page(YONGXIN, YONGXIN_RULES, older=True)
        if not r.stated_total
    ]
    assert read[:2] == [("cninfo:分行业", "其它塑料制造业"), ("cninfo:分产品", "彩印复合包装材料")]
    assert not any("关联交易" in name for _, name in read)


def test_a_row_of_labels_after_the_rows_ends_the_table() -> None:
    """太钢不锈's share table, its amounts moved into the revenue column: the label row above
    them (`主营业务收入 | 主营业务利润`) ends the product table before they are read."""
    moved = [
        TextItem(i.s, i.x - 40, i.y, None if i.end is None else i.end - 40)
        if i.y < 520 and 190 <= i.x <= 200 and i.s[:1].isdigit()
        else i
        for i in TAIGANG
    ]
    assert [r[1] for r in rows(moved) if r[0] == "cninfo:分产品"] == [
        "不锈钢材",
        "普通钢材",
        "普通钢坯",
        "其他",
    ]


def test_a_figure_set_under_a_one_line_header_is_a_row_whose_name_is_above_it() -> None:
    """大同煤业's region row with its name lifted onto a line of its own: the figure line, 13.6 pt
    under the header, holds no name, and is still no part of the header."""
    lifted = [TextItem(i.s, i.x, i.y + 7, i.end) if i.s == "国内" else i for i in DATONG]
    assert rows(lifted)[-1] == ("cninfo:分地区", "国内", 10322197979.55, None)


def test_a_name_label_split_mid_word_over_two_lines_is_found() -> None:
    read = rows(FENGHUANG)
    assert [r[1] for r in read if r[0] == "cninfo:分行业"] == ["制造业", "商业", "合并抵消"]
    assert [r[1] for r in read if r[0] == "cninfo:分产品"] == [
        "光学加工",
        "精密加工",
        "照相器材",
        "合并抵消",
    ]
    stated = [
        r.revenue for r in cn_pdf.parse_segment_page(FENGHUANG, [], older=True) if r.stated_total
    ]
    assert stated == [1362580289.78, 1362580289.78]


def test_whole_yuan_figures_are_read_on_a_page_that_prints_no_fen() -> None:
    assert rows(ZPMC)[:2] == [
        ("cninfo:分行业", "集装箱起重机", 8576009665.0, 8496041195.0),
        ("cninfo:分行业", "散货机械", 3690151188.0, 3834464868.0),
    ]
    assert len(rows(ZPMC)) == 5


@pytest.mark.parametrize(
    ("line", "axis"),
    [
        ("(1) 主营业务分行业、产品情况", "cninfo:分行业"),
        ("①主营业务分行业、分产品情况表", "cninfo:分行业"),
        ("（1）主营业务分行业、产品、主营业务利润情况：", "cninfo:分行业"),
        ("主营业务收入分行业情况", "cninfo:分行业"),
        ("主营业务分产品情况", "cninfo:分产品"),
        ("(2)主营业务分地区情况", "cninfo:分地区"),
        ("1、分行业经营情况（单位：人民币万元）", "cninfo:分行业"),
        ("2、按主要产品列示的经营情况单位:万元", "cninfo:分产品"),
        ("① 报告期主营业务分行业情况：", "cninfo:分行业"),
        ("公司主营业务分行业情况良好，收入稳步增长", None),
        ("主营业务分行业情况的说明", None),
        ("（二）行业发展情况", None),
    ],
)
def test_an_older_heading_is_a_line_of_its_own(line: str, axis: str | None) -> None:
    assert cn_pdf._older_axis(line) == axis


@pytest.mark.parametrize(
    "cover",
    [
        "安徽皖通高速公路股份有限公司 2010 年 年 度 报 告 2011年3月11日 公司2009年年度报告",
        "山西太钢不锈钢股份有限公司2010年 度 报 告二○一一年四月二十七日",
        "第1页深圳市金证科技股份有限公司二○一○年度报告正文二○一一年四月二十六日",
    ],
)
def test_the_year_of_a_cover_set_letter_spaced_or_in_chinese_numerals(cover: str) -> None:
    assert cn_pdf.fiscal_year_end_from(cover) == "2010-12-31"


def test_a_bracketed_currency_in_the_unit_line_still_names_the_unit() -> None:
    found = cn_pdf._UNIT.search("单位：（人民币）万元")
    assert found is not None and found.group(1) == "万元"
