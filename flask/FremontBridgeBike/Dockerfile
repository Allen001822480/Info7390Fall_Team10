From python:3
RUN pip install --upgrade pip
RUN pip install Flask
RUN pip install python-forecastio
RUN pip install holidays
RUN pip install pandas
RUN pip install seaborn
RUN pip install Flask-WTF==0.14.2
RUN pip install scikit-learn==0.20.0
#Add requirements.txt
#ADD requirements.txt /
#RUN pip install -r requirements.txt


RUN cd

#Create static directory
RUN mkdir “static”

#Add Required files

ADD static/main.css /static/

#Create templates directory
RUN mkdir “templates”

#Add Required files

ADD templates/home.html /templates/
ADD templates/layout.html /templates/
ADD templates/result.html /templates/

#Create templates directory
RUN mkdir “prediction”

#Add Required files

ADD prediction/final.py /prediction/
ADD prediction/sample.csv /prediction/


ADD gaoshunan_east_model.sav /
ADD gaoshunan_west_model.sav /
ADD flaskblog.py /
ADD forms.py /

CMD python flaskblog.py
