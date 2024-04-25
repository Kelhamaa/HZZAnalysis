FROM python:latest

ADD output/output_run_functions.py ./

RUN pip install --upgrade awkward numpy matplotlib pika

CMD ["python", "./output_run_functions.py"]