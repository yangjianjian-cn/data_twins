from dateutil.parser import parse
from collections import Counter


def detect_date_format(samples):
    samples = samples.astype(str).dropna()
    if samples.empty:
        return None

    candidate_formats = [
        "%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d",
        "%Y%m%d",  # 无分隔符
        "%d-%m-%Y", "%d/%m/%Y", "%d.%m.%Y",
        "%m-%d-%Y", "%m/%d/%Y", "%m.%d.%Y",
        "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y.%m.%d %H:%M:%S",
        "%Y%m%d%H%M%S"  # 例如 20250405143000
    ]

    matched_formats = []

    for s in samples:
        try:
            dt = parse(s)
            for fmt in candidate_formats:
                if dt.strftime(fmt) == s:
                    matched_formats.append(fmt)
                    break
        except Exception as e:
            continue

    if matched_formats:
        # 返回最常见的格式
        return Counter(matched_formats).most_common(1)[0][0]
    return None


data = """
289075358
289024337
289024769
289075357
289024336
328135504
328186836
230689396
230739679
273265264
273214482
241485899
241536232
328135505
328186837
241534460
241484058
328183043
328131592
328134718
328186065
264842419
264791733
328131747
241534896
241484496
328183039
328131588
328133623
328185005
327740123
327688635
328282420
328333752
328282421
328333753
328278508
328329959
328332981
328281634
328278663
328329955
328278504
328331921
328280539
328421825
328473157
307134920
307083814
307134917
""".strip().split()

# 打印结果查看
print(data)

import pandas as pd

s = pd.Series(data)
print(detect_date_format(s))
# 输出: %Y-%m-%d
