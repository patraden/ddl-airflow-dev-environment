## homework
should you have any questions - denis.patrakhin@gmail.com

## preambule
По первому заданию реализовано 2 ETL сценария загрузки валютной пары BTC/USD c https://exchangerate.host/
1. Историческая загрузка c произвольной даты (максимальная историчность api - 1999-02-01) до даты активации DAG
2. Инкрементальная загрузка с 00:00:00 даты активации DAG c загрузкой каждые 3 часа
Второе задание так же выполнено через airflow в виде DAG-а выгрузки необходимых csv файлов.

Общие комментарии по реализации:
* Общий принцип данной разработки - KISS (максимально просто и не "изобретая велосипедов")
* Для простоты airflow таски выполняются на LocalExeutor
* Так же для упрощения я не писал тестов
* Никаких кастомных операторов с красивыми шаблонами и пр. (хотя кое-где они напрашиваются, например, для шаблонов второго задания)
* Не смешиваем историческую загрузку с инкрементальной в одном DAG
* Обработка данных максимально простая (без pandas, spark и т.д.)
* Расширяем docker image через _PIP_ADDITIONAL_REQUIREMENTS (и не паримся)

Комментарии по clickhouse:
* К сожалению, с clickhouse я до настоящего момента не работал (это первый, пробный опыт)
* В качестве движка таблицы "сырых" данных решил использовать Log, поскольку: 
    1. Хотелось в качесте raw слоя что-то не из MergeTree family :) 
    2. Пишет быстро
    3. Размер датасета небольшой
    4. Кажется логичным для сценария когда мы записываем 8 версий курса валютной пары в день (хотя API предусматривает 1 значение в день)
* В качестве целевой (optimized) таблицы используем ReplacingMergeTree с версионностью, предполагая, что:
    1. По факту в день нужно только 1 значение курса валюты, поэтому остальные надо дедублицировать
    2. Индексы настраиваем на дату и валютную пару, поскольку скорее всего основная масса запросов вида: "курс для пару на дату/в период"
    3. В качестве колонки версии берем время загрузки значения (таким образом, таблица каждый день будет хранить последнее обновление в этот день, в том числе время загрузки исторических данных)

## setup
```bash
# assume that ariflow docker and docker-compose requirements have been met and you operate *unix base OS (or at least win wsl2 with bash shell).
# https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html

# provide relevant test_db connection details
export TEST_DB_HOST=<...>
export TEST_DB_PORT=<...>
export TEST_DB_NAME=<...>
export TEST_DB_USER=<...>
export TEST_DB_PASSWORD=<...>

# create all relevant configurations, including db init script
chmod u+x setup.sh
./setup.sh

# init and start airflow
docker-compose up airflow-init
docker-compose up -d
```

## validation
* После полной загрузки airflow нужно активировать все 3 DAG через web interface (ariflow:airflow)
* Результаты первого задания будут достуны с localhost для любого клиента (dbeaver?). Либо через докер ```docker exec -it homework-dwh-1 clickhouse-client```
* Результаты второго задания будут доступны в папке ./storage