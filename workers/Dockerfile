FROM python:latest

ADD infofile.py workers/processing_functions.py ./

RUN pip install --upgrade uproot awkward vector numpy pika

CMD ["python", "./processing_functions.py"]