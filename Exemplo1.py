import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
 
Exemplo1 = DAG(
	dag_id="Exemplo1",
	start_date=datetime.now(), # apenas para teste
	schedule_interval=None,
)

generate_data = BashOperator(
	task_id="generate_data",
	bash_command=
		'path=/tmp/airflow-example1 && '
		'filename="$path"/"{{ds}}".csv && '
		'mkdir -p "$path" && '
		'echo -e "nome,cidade\nCleber,Osasco\nMatilde,Itapecerica da Serra\nRoberval,Osasco\nGilean,Blumenau\nJucicleide,Osasco" > "$filename" && '
		'echo "$filename"',
	do_xcom_push=True,
	dag=Exemplo1,
)

def _parse_data(**context):
	filename = context["task_instance"].xcom_pull(
		task_ids="generate_data", key="return_value"
	)
	print(f"file {filename}")
	import pandas
	df = pandas.read_csv(filename)
	print(df)
	output = filename.replace('.csv', '_cidades.csv')
#	dfHabitantesPorCidade = df['cidade'].value_counts()
	dfHabitantesPorCidade = df.rename(columns={'nome':'habitantes'}).groupby(["cidade"]).count()
#	print(dfHabitantesPorCidade)
	dfHabitantesPorCidade.to_csv(output, index=True)

parse_data = PythonOperator(
	task_id="parse_data", 
	python_callable=_parse_data,
	dag=Exemplo1,
)

generate_data >> parse_data
