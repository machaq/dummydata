import yaml
from dataclasses import dataclass, make_dataclass
import factory
import pandas as pd
from faker import Faker
from datetime import datetime
import random

# Fakerインスタンス
faker = Faker()


# YAMLファイルの読み込み
def load_config_and_models(yaml_file):
    with open(yaml_file, "r") as file:
        data = yaml.safe_load(file)
    return data["config"], data["models"]


# フィールドタイプの解析
def parse_field_type(field_type):
    if "str" in field_type:
        if "(" in field_type:
            params = field_type.split("(")[1].rstrip(")")
            constraints = dict(param.split("=") for param in params.split(", "))
            return "str", {
                "min": int(constraints.get("min", 1)),
                "max": int(constraints.get("max", 255)),
            }
        return "str", {}
    elif field_type == "timestamp":
        return "timestamp", {}
    return field_type, {}


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
            if field_type == "str":
                processed_fields.append((name, str))
            elif field_type == "timestamp":
                processed_fields.append((name, datetime))
            else:
                processed_fields.append((name, eval(field_type)))
        dataclasses[model_name] = make_dataclass(model_name, processed_fields)
    return dataclasses


# 各データクラスに対するFactoryの生成
# 各データクラスに対するFactoryの生成
def create_factories(dataclasses, models):
    factories = {}

    for model_name, dataclass in dataclasses.items():
        fields = models[model_name]
        factory_fields = {}

        for field in fields:
            if isinstance(field, str):
                name, type_def = field.split(": ")
            elif isinstance(field, dict):
                name, type_def = list(field.items())[0]

            field_type, constraints = parse_field_type(type_def)

            if field_type == "str":
                min_len = max(constraints.get("min", 1), 5)  # min_lenは5以上
                max_len = constraints.get("max", 255)
                result = "?" * random.randint(min_len, max_len)
                factory_fields[name] = factory.LazyFunction(
                    lambda min_len=min_len, max_len=max_len: faker.lexify(result)
                )
            elif field_type == "timestamp":
                factory_fields[name] = factory.LazyFunction(
                    lambda: faker.date_time_between(start_date="-10y", end_date="now")
                )
            elif field_type == "int":
                factory_fields[name] = factory.Sequence(lambda n: n + 1)

        # 動的にFactoryクラスを作成し登録
        class Meta:
            model = dataclass

        factories[model_name] = type(
            f"{model_name}Factory", (factory.Factory,), {**factory_fields, "Meta": Meta}
        )
    return factories


# メイン処理
def main():
    # YAMLファイルから設定とモデルを読み込み
    config, models = load_config_and_models("models.yml")

    # データクラスの生成
    dataclasses = create_dataclass_from_yaml(models)

    # 各データクラスに対応するFactoryクラスの生成
    factories = create_factories(dataclasses, models)

    # Factoryクラスの取得
    ParentFactory = factories["Parent"]
    ChildFactory = factories["Child"]

    # 設定値の取得
    parent_config = config["Parent"]
    child_config = config["Child"]

    # データ生成
    parents = []
    children = []

    for _ in range(parent_config["rows"]):  # Parentの行数
        parent = ParentFactory()
        parents.append(
            {
                "id": parent.id,
                "name": parent.name,
                "created_at": parent.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

        for _ in range(child_config["rows_per_parent"]):  # 各Parentに紐付くChildの行数
            child = ChildFactory(parent_id=parent.id)
            children.append(
                {
                    "id": child.id,
                    "name": child.name,
                    "hobby": child.hobby,
                    "birth_date": child.birth_date.strftime("%Y-%m-%d %H:%M:%S"),
                    "parent_id": child.parent_id,
                }
            )

    # CSVエクスポート
    pd.DataFrame(parents).to_csv(parent_config["output_file"], index=False)
    pd.DataFrame(children).to_csv(child_config["output_file"], index=False)
    print("CSVファイルが作成されました！")


if __name__ == "__main__":
    main()
