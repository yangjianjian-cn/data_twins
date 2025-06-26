import json
import requests
from faker import Faker
import random
from datetime import datetime
from typing import List, Any, Dict, Optional, Union
import ast
from datetime import date

# 设置ollama服务的URL
OLLAMA_URL = 'http://111.231.0.147:11434/api/generate'

# 初始化Faker
fake = Faker('zh_CN')


def classify_data(
        sample_group: List[Any],
        categories: List[str],
        examples: Optional[Dict[str, List[str]]] = None,
        max_input_length: int = 1000
) -> str:
    """
    使用大语言模型将单个数据样本组分类为给定类别之一。
    假定sample_group中的所有项目属于同一类别。

    Args:
        sample_group: 包含单个分类任务样本的列表
        categories: 类别名称列表
        examples: 可选的{category: [example1, example2, ...]}字典，用于指导分类
        max_input_length: 发送给模型的输入的最大长度

    Returns:
        分类结果字符串（英文）
    """
    # 中英文类别映射
    category_mapping = {
        "地址": "address",
        "省名": "province",
        "城市": "city",
        "银行名称": "bank_name",
        "公司名称": "company_name",
        "信用卡号": "credit_card_number",
        "日期时间": "date_time",
        "人名": "person_name",
        "电话号码": "phone_number",
        "邮件地址": "email",
        "其他": "other"
    }

    # Prepare examples string if provided
    examples_str = ""
    if examples:
        examples_str = "以下是一些分类的例子:\n"
        for category, exs in examples.items():
            examples_str += f"{category}: {', '.join(exs)}\n"

    # Prepare sample group for prompt, limiting its length
    def json_serializable(obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return str(obj)

    sample_group_str = json.dumps(sample_group, ensure_ascii=False, default=json_serializable)
    if len(sample_group_str) > max_input_length:
        sample_group_str = sample_group_str[:max_input_length] + "..."

    prompt = f"""请将以下数据样本组准确分类为以下类别之一，且只返回一个类别：{', '.join(categories)}。如果有任何疑惑或不属于这些类别，请回答'其他'。
        所有样本以列表形式给出，用","分割，都属于同一个类别，请将整个样本组作为一个整体来分类。如果样本组包含多个类别，请返回出现次数最多的类别。
        {examples_str}
        数据样本组:
        {sample_group_str}
        请只返回一个分类结果，不要包含任何额外的解释、标点符号或格式。你的回答应该只包含一个词，即分类结果。"""

    payload = {
        "model": "gemma2:2b",
        "prompt": prompt,
        "stream": False
    }

    response = requests.post(OLLAMA_URL, json=payload)
    if response.status_code == 200:
        result = response.json()['response'].strip()

        # Validate and parse the classification
        valid_categories = set(categories + ['其他'])
        if result in valid_categories:
            return category_mapping.get(result, 'other')
        else:
            # Try to match the result with a valid category
            for category in valid_categories:
                if category in result:
                    print(f"Warning: Parsed '{category}' from model response '{result}'.")
                    return category_mapping.get(category, 'other')
            print(f"Warning: Invalid classification '{result}'. Returning 'other'.")
            return 'other'
    else:
        raise Exception(f"Failed to get response from Ollama service. Status code: {response.status_code}")


def detect_datetime_format(sample: str) -> Optional[str]:
    """
    检测日期时间字符串的格式。

    Args:
        sample: 要检测格式的日期时间字符串

    Returns:
        检测到的日期时间格式字，如果无法检测则返回None
    """
    formats = [
        "%Y%m%d",
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d",
        "%Y/%m/%d %H:%M:%S",
        "%d-%m-%Y",
        "%d-%m-%Y %H:%M:%S",
        "%d/%m/%Y",
        "%d/%m/%Y %H:%M:%S",
        "%Y年%m月%d日",
        "%Y年%m月%d日 %H时%M分%S秒",
        "%Y年%m月%d日 %H时%M分",
        "%Y年%m月%d日 %H时",
        "%Y年%m月%d日",
        "%Y年%m月"
    ]

    for fmt in formats:
        try:
            datetime.strptime(sample, fmt)
            return fmt
        except ValueError:
            continue
    return None


def generate_similar_data(
        category: str,
        samples: List[Any],
        num_generate: int = 20,
        use_llm_for_unknown: bool = False
) -> List[Any]:
    """
    根据给定的类别和样本生成相似的数据。

    Args:
        category: 数据类别（英文）
        samples: 原始样本数据列表
        num_generate: 要生成的数据项数量
        use_llm_for_unknown: 是否使用大语言模型生成未知类型的数据

    Returns:
        生成的数据列表
    """
    generated_data = []

    if category != "other" or not use_llm_for_unknown:
        for _ in range(num_generate):
            if category == "address":
                generated_data.append(fake.address())
            elif category == "province":
                generated_data.append(fake.province())
            elif category == "city":
                generated_data.append(fake.city())
            elif category == "bank_name":
                generated_data.append(fake.company() + "银行")
            elif category == "company_name":
                generated_data.append(fake.company())
            elif category == "credit_card_number":
                generated_data.append(fake.credit_card_number())
            elif category == "date_time":
                sample_format = detect_datetime_format(random.choice(samples))
                if sample_format:
                    generated_data.append(fake.date_time().strftime(sample_format))
                else:
                    generated_data.append(fake.date_time().strftime("%Y-%m-%d %H:%M:%S"))
            elif category == "person_name":
                generated_data.append(fake.name())
            elif category == "phone_number":
                generated_data.append(fake.phone_number())
            elif category == "email":
                generated_data.append(fake.email())
            else:
                # 对于"other"类别，仿样本的格式
                sample = random.choice(samples)
                if isinstance(sample, str):
                    generated_data.append(fake.pystr(min_chars=len(sample), max_chars=len(sample)))
                elif isinstance(sample, (int, float)):
                    min_val = min(samples)
                    max_val = max(samples)
                    decimal_places = max(len(str(x).split('.')[-1]) for x in samples if isinstance(x, float))
                    generated_data.append(round(random.uniform(min_val, max_val), decimal_places))
                else:
                    generated_data.append(str(sample))
    else:
        # 使用大模型生成类似的数据
        generated_data = generate_data_with_llm(samples, num_generate)

    return generated_data


def generate_data_with_llm(samples: List[Any], num_generate: int) -> List[str]:
    """
    使用大语言模型生成与给定样本相似的数据。

    Args:
        samples: 原始样本数据列表
        num_generate: 要生成的数据项数量

    Returns:
        生成的数据列表
    """
    generated_data = []
    batch_size = 10

    for i in range(0, num_generate, batch_size):
        current_batch_size = min(batch_size, num_generate - i)
        # 将前一次生成的数据作为样本输入
        current_samples = samples + generated_data[-1 * batch_size:]
        prompt = f"""根据以下样本数据，生成{current_batch_size}个相似的数据项。
        1. 生成的数据与样本在格式和结构上相似，但内容不同。
        2. 不要生成有规律的字符，不要生成重复的样本数据。
        3. 不需要用引号将字符串括起来。

        样本数据：
        {json.dumps(current_samples, ensure_ascii=False)}

        请生成{current_batch_size}个类似的数据项，每行一个，不要包含任何额外的解释或标记。"""

        payload = {
            # "model": "gemma2:2b",
            "model": "gemma2:latest",
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0,
            }
        }

        response = requests.post(OLLAMA_URL, json=payload)
        if response.status_code == 200:
            result = response.json()['response'].strip()
            try:
                # 尝试将结果解析为Python列表
                new_data = ast.literal_eval(result.strip('```').strip('\n'))[:current_batch_size]
                generated_data.extend(new_data)
            except (ValueError, SyntaxError):
                # 如果解析失败，则按原方式处理
                new_data = result.strip('```').strip('\n').split('\n')[:current_batch_size]
                generated_data.extend(new_data)
        else:
            raise Exception(f"Failed to get response from Ollama service. Status code: {response.status_code}")

    return generated_data


def analyze_and_generate(
        samples: List[Any],
        num_generate: int = 20,
        use_llm_for_unknown: bool = False
) -> Dict[str, Any]:
    """
    分析给定的样本数据并生成相似的新数据。

    Args:
        samples: 原始样本数据列表
        num_generate: 要生成的数据项数量
        use_llm_for_unknown: 是否使用大语言模型生成未知类型的数据

    Returns:
        包含类型和生成数据的字典
    """
    categories = ["地址", "省名", "城市", "银行名称", "公司名称", "信用卡号", "日期时间", "人名", "电话号码", "邮件地址", "其他"]
    examples = {
        "地址": ["上海市浦东新区张杨路500号", "广东省深圳市南山区科技园"],
        "银行名称": ["工商银行", "建设银行"],
        "日期时间": ["2022-01-01 00:00:00", "2023-12-31 23:59:59"],
        "电话号码": ["010-12345678", "18911112222"],
        "人名": ["张三", "李四"],
        "邮件地址": ["windows@yahoo.com", "linux@gmail.com"],
    }

    # 对样本数据进行分类
    category = classify_data(samples, categories, examples)
    print(f"Classified category: {category}")

    # 生成新数据
    generated_data = generate_similar_data(category, samples, num_generate, use_llm_for_unknown)

    return {
        "type": category,
        "stats": {"generated_data": generated_data}
    }


def analyze_llm_field(table: str, column: str, sample_data: List[Any]) -> str:
    """
    使用大语言模型分析字段类型。

    Args:
        table: 表名
        column: 列名
        sample_data: 样本数据列表

    Returns:
        字段类型（字符串）
    """
    categories = ["地址", "省名", "城市", "银行名称", "公司名称", "信用卡号", "日期时间", "人名", "电话号码", "邮件地址", "其他"]
    examples = {
        "地址": ["上海市浦东新区张杨路500号", "广东省深圳市南山区科技园"],
        "银行名称": ["工商银行", "建设银行"],
        "日期时间": ["2022-01-01 00:00:00", "2023-12-31 23:59:59"],
        "电话号码": ["010-12345678", "18911112222"],
        "人名": ["张三", "李四"],
        "邮件地址": ["windows@yahoo.com", "linux@gmail.com"],
    }
    return classify_data(sample_data, categories, examples)


if __name__ == "__main__":
    # 样例数据（假设这些都是同一类型）
    sample_data = [
        "6226994378876444",
        "6226933054968744"
    ]
    sample_data = [
        "133655445588",
        "189111122228"
    ]
    sample_data = [
        "sk-cizpgelwhisfizscbwsfelyhzddcenxwfquwwlhjziakukko",
        "sk-ckdslakdslakdkaklfjiewoewjewomfkpewkdsnhjreiofdk",
        "sk-ioewiemvmkkdshgeorwoslkgqfekqlfjdajdisafdjsjnndd",
    ]
    sample_data = [
        "DECT9-UIJG3-E5MJU-UY8JK-UI6YT",
        "XNTT9-CWMM3-RM2YM-D7KB2-JB6DV"
    ]
    sample_data = [
        "QQ-CN0032",
        "XX-ER0033",
        "BB-WC0032",
        "YX-LO0032",
        "HW-JH0032",
    ]
    sample_data = [
        "ZHCN-iidk-0032",
        "EERN-hhli-4335",
        "ZTTN-hfdk-0654",
        "ENUS-iotr-9587"
    ]
    # 生成新数据，使用大模型生成未知类型的数据
    generated_data = analyze_and_generate(sample_data, 20, use_llm_for_unknown=True)

    # 打印生成的数据
    print(f"Type: {generated_data['type']}")
    print(f"Generated Data:\n{''.join(generated_data['stats']['generated_data'])}")
