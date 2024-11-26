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
def load_models_from_yaml(yaml_file):
    with open(yaml_file, 'r') as file:
        data = yaml.safe_load(file)
    return data['models']

# フィールドタイプの解析
def parse_field_type(field_type):
    if 'str' in field_type:
        if '(' in field_type:
            params = field_type.split('(')[1].rstrip(')')
            constraints = dict(param.split('=') for param in params.split(', '))
            return 'str', {'min': int(constraints.get('min', 1)), 'max': int(constraints.get('max', 255))}
        return 'str', {}
    elif field_type == 'timestamp':
        return 'timestamp', {}
    return field_type, {}

# データモデルの動的生成
def create_dataclass_from_yaml(models):
    dataclasses = {}
    for model_name, fields in models.items():
        processed_fields = []
        for field in fields:
            name, type_def = field.split(': ')
            field_type, constraints = parse_field_type(type_def)
            if field_type == 'str':
                processed_fields.append((name, str))
            elif field_type == 'timestamp':
                processed_fields.append((name, datetime))
            else:
                processed_fields.append((name, eval(field_type)))
        dataclasses[model_name] = make_dataclass(model_name, processed_fields)
    return dataclasses

# Factoryの動的生成
def create_factory_for_model(model_name, model_class, models):
    class DynamicFactory(factory.Factory):
        class Meta:
            model = model_class

    # 属性に応じたルールの追加
    for field in models[model_name]:
        name, type_def = field.split(': ')
        field_type, constraints = parse_field_type(type_def)

        if field_type == 'str':
            min_len = constraints.get('min', 1)
            max_len = constraints.get('max', 255)
            setattr(DynamicFactory, name, factory.Faker('lexify', text='?' * max_len)[:random.randint(min_len, max_len)])
        elif field_type == 'timestamp':
            setattr(DynamicFactory, name, factory.Faker('date_time'))
        elif field_type == 'int':
            setattr(DynamicFactory, name, factory.Sequence(lambda n: n + 1))
    return DynamicFactory

# メイン処理
def main():
    # モデル定義の読み込み
    models = load_models_from_yaml('models.yml')

    # データクラスの生成
    dataclasses = create_dataclass_from_yaml(models)

    # Factoryの生成
    CompanyFactory = create_factory_for_model("Company", dataclasses["Company"], models)
    EmployeeFactory = create_factory_for_model("Employee", dataclasses["Employee"], models)

    # データ生成
    companies = []
    employees = []

    for _ in range(5):  # 5社生成
        company = CompanyFactory()
        companies.append({
            'id': company.id,
            'name': company.name,
            'created_at': company.created_at.strftime('%Y-%m-%d %H:%M:%S')
        })

        for _ in range(10):  # 各社に10人の従業員を生成
            employee = EmployeeFactory(company_id=company.id)
            employees.append({
                'id': employee.id,
                'name': employee.name,
                'position': employee.position,
                'hired_at': employee.hired_at.strftime('%Y-%m-%d %H:%M:%S'),
                'company_id': employee.company_id
            })

    # CSVエクスポート
    pd.DataFrame(companies).to_csv('companies.csv', index=False)
    pd.DataFrame(employees).to_csv('employees.csv', index=False)
    print("CSVファイルが作成されました！")

if __name__ == "__main__":
    main()
