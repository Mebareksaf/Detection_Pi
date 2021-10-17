FROM mohaseeb/raspberrypi3-python-opencv

WORKDIR /usr/app

ADD ./requirements.txt ./ 
RUN wget https://github.com/edenhill/librdkafka/archive/refs/tags/v1.7.0.tar.gz && \ 
tar xvf v1.7.0.tar.gz 
RUN cd librdkafka-1.7.0 && \ 
./configure --prefix=/usr && \ 
make -j4 && \ 
make install
RUN cd .. 
RUN pip install -r requirements.txt
ADD ./ ./

CMD ["python", "app.py"]
