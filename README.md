# pet_de_f1

windows

py -m venv .venv

.\.venv\Scripts\Activate.ps1

docker-compose -f docker-compose_af2.yaml -p pet_de_f1_af2 up -d

pip install -r req.txt

docker exec -it pet_de_f1_af2-airflow-worker-1 airflow dags backfill --continue-on-failures --rerun-failed-tasks -s '2024-01-01' -e ' 2024-04-03' raw_to_s3