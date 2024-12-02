import yaml
from dataclasses import make_dataclass, asdict
import factory
import pandas as pd
from faker import Faker
from datetime import datetime
import random

faker = Faker()

# YAMLファイルの読み込み
def load_config_and_models(yaml_file):
    with open(yaml_file, "r") as file:
        data = yaml.safe_load(file)
    return data["config"], data["models"]

# フィールドタイプの解析
def parse_field_type(field_type):
    if "str_seq" in field_type:
        return "str_seq", parse_constraints(field_type, str, {"prefix": "", "start": 1})
    elif "str" in field_type:
        return "str", parse_constraints(field_type, int, {"min": 1, "max": 255})
    elif "float" in field_type:
        return "float", parse_constraints(
            field_type, float, {"min": 0.0, "max": 1000.0}
        )
    elif "boolean" in field_type:
        return "boolean", parse_constraints(field_type, float, {"true_ratio": 0.5})
    elif "count" in field_type:
        return "count", parse_constraints(field_type, int, {"start": 1})
    elif "int" in field_type:
        return "int", parse_constraints(field_type, int, {"min": 1, "max": 100})
    elif "timestamp" in field_type:
        return "timestamp", {}
    else:
        raise ValueError(f"Unsupported field type: {field_type}")

# 制約の解析
def parse_constraints(field_type, cast_type, default_constraints):
    constraints = default_constraints.copy()
    if "(" in field_type:
        params = field_type.split("(", 1)[1].rstrip(")").split(", ")
        constraints.update(
            {k: v for k, v in (param.split("=", 1) for param in params)}
        )
    return constraints

# データモデルの動的生成
def create_dataclass_from_yaml(models):
    dataclasses = {}
    for model_name, fields in models.items():
        processed_fields = []
        for field in fields:
            if isinstance(field, str):
                name, type_def = field.split(": ")
            elif isinstance(field, dict):
                name, type_def = list(field.items())[0]
            field_type, _ = parse_field_type(type_def)
            python_type = resolve_type(field_type)
            processed_fields.append((name, python_type))
        dataclasses[model_name] = make_dataclass(model_name, processed_fields)
    return dataclasses

# 型解決
def resolve_type(field_type):
    return {
        "str": str,
        "float": float,
        "boolean": bool,
        "int": int,
        "count": int,
        "timestamp": datetime,
        "str_seq": str,
    }.get(field_type, None)

# 各データクラスに対するFactoryの生成
def create_factories(dataclasses, models):
    factories = {}
    counters = {}  # count型のためのカウンターを保持

    for model_name, dataclass in dataclasses.items():
        fields = models[model_name]
        factory_fields = {}

        for field in fields:
            name, type_def = (
                field.split(": ") if isinstance(field, str) else list(field.items())[0]
            )
            field_type, constraints = parse_field_type(type_def)

            factory_fields[name] = generate_factory_field(
                field_type, constraints, counters, name
            )

        class Meta:
            model = dataclass

        factories[model_name] = type(
            f"{model_name}Factory", (factory.Factory,), {**factory_fields, "Meta": Meta}
        )
    return factories

# 各フィールドに対応するFactoryの生成
def generate_factory_field(field_type, constraints, counters, field_name):
    if field_type == "str_seq":
        prefix = constraints.get("prefix", "")
        start = int(constraints.get("start", 1))
        return factory.LazyFunction(
            lambda: generate_sequential_string(counters, field_name, prefix, start)
        )
    elif field_type == "str":
        min_len, max_len = int(constraints["min"]), int(constraints["max"])
        return factory.LazyFunction(
            lambda: faker.lexify("?" * random.randint(min_len, max_len))
        )
    elif field_type == "float":
        return factory.LazyFunction(
            lambda: random.uniform(float(constraints["min"]), float(constraints["max"]))
        )
    elif field_type == "boolean":
        return factory.LazyFunction(lambda: random.random() < float(constraints["true_ratio"]))
    elif field_type == "timestamp":
        return factory.LazyFunction(
            lambda: faker.date_time_between(start_date="-10y", end_date="now")
        )
    elif field_type == "count":
        return factory.LazyFunction(
            lambda: increment_counter(counters, field_name, int(constraints["start"]))
        )
    elif field_type == "int":
        return factory.LazyFunction(
            lambda: random.randint(int(constraints["min"]), int(constraints["max"]))
        )

# フィールド値を直接生成する関数
def generate_field_value(field_type, constraints, counters, field_name):
    if field_type == "str_seq":
        prefix = constraints.get("prefix", "")
        start = int(constraints.get("start", 1))
        return generate_sequential_string(counters, field_name, prefix, start)
    elif field_type == "str":
        min_len, max_len = int(constraints["min"]), int(constraints["max"])
        return faker.lexify("?" * random.randint(min_len, max_len))
    elif field_type == "float":
        return random.uniform(float(constraints["min"]), float(constraints["max"]))
    elif field_type == "boolean":
        return random.random() < float(constraints["true_ratio"])
    elif field_type == "timestamp":
        return faker.date_time_between(start_date="-10y", end_date="now")
    elif field_type == "count":
        return increment_counter(counters, field_name, int(constraints["start"]))
    elif field_type == "int":
        return random.randint(int(constraints["min"]), int(constraints["max"]))

# カウンターをインクリメントする関数
def increment_counter(counters, field_name, start):
    if field_name not in counters:
        counters[field_name] = start
    value = counters[field_name]
    counters[field_name] += 1
    return value

# 連番付き文字列を生成する関数
def generate_sequential_string(counters, field_name, prefix, start):
    if field_name not in counters:
        counters[field_name] = start
    value = f"{prefix}{counters[field_name]}"
    counters[field_name] += 1
    return value

def main():
    config, models = load_config_and_models("models.yml")

    dataclasses = create_dataclass_from_yaml(models)
    parent_fields = [
        list(field.keys())[0] if isinstance(field, dict) else field.split(": ")[0]
        for field in models["Parent"]
    ]
    child_fields = [
        list(field.keys())[0] if isinstance(field, dict) else field.split(": ")[0]
        for field in models["Child"]
    ]
    factories = create_factories(dataclasses, models)

    ParentFactory = factories["Parent"]
    ChildFactory = factories["Child"]

    parent_config = config["Parent"]
    child_config = config["Child"]

    parents = []
    children = []

    counters = {}  # カウンターを初期化

    for _ in range(parent_config["rows"]):
        parent = ParentFactory()
        parent_data = asdict(parent)
        parents.append({key: parent_data[key] for key in parent_fields})

        for _ in range(child_config["rows_per_parent"]):
            child_data = {}
            for key in child_fields:
                if key in parent_fields:
                    child_data[key] = parent_data[key]  # 親の値をコピー
                else:
                    # 子のフィールドを生成
                    field = next(
                        (f for f in models["Child"] if (f.split(": ")[0] if isinstance(f, str) else list(f.keys())[0]) == key),
                        None
                    )
                    if field:
                        name, type_def = (
                            field.split(": ") if isinstance(field, str) else list(field.items())[0]
                        )
                        field_type, constraints = parse_field_type(type_def)
                        child_data[key] = generate_field_value(
                            field_type, constraints, counters, name
                        )
            child_data['parent_id'] = parent_data['id']  # 親IDを設定
            children.append(child_data)

    # CSVエクスポート
    pd.DataFrame(parents).to_csv(parent_config["output_file"], index=False)
    pd.DataFrame(children).to_csv(child_config["output_file"], index=False)
    print("CSVファイルが作成されました！")

if __name__ == "__main__":
    main()
