FROM python:latest

ADD infofile.py operators/run_functions.py /

RUN pip install --upgrade pika

CMD ["python", "/run_functions.py"]