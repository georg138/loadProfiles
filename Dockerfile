FROM python:3.12-bookworm

WORKDIR /app

RUN pip install --upgrade pip
COPY ./requirements.txt /app/requirements.txt
#RUN apt update
#RUN apt install -y python3-numpy
RUN pip install -r requirements.txt 

EXPOSE 5000

# copy project
COPY costEstimate.py /app/

CMD [ "flask", "--app", "/app/costEstimate.py", "run", "--host=0.0.0.0" ]
