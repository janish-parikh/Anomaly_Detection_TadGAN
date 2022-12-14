# Anomaly_Detection_TadGAN
Houses code for an anomaly detection pipeline that can detect anomalies in time series data in real-time using TadGAN, Spark and Kafka

# Abstract
Time series anomalies can offer information rel-
evant to critical situations facing various fields,
from finance and aerospace to the IT, security,
and medical domains. We apply one such model,
TadGAN (Geiger et al., 2020) which is built
on top of GANs and is meant for unsupervised
anomaly detection in time-series data. TadGAN
has already improved performances on six out of
eleven benchmark datasets such as NASA, Yahoo-
S5 and NAB.
We primarily build an anomaly detection pipeline
that can detect anomalies in time series data in
real-time. To establish this, we explore tools such
as Spark Structured Streaming, Kafka and Parquet
file sink to build an anomaly detection pipeline.
All of the benchmark datasets of TADGAN are
univariate i.e a single signal is being used to re-
generate single signal. We extend its use to other
datasets across domains using the multivariate
SKAB dataset and compare results obtained by
TADGAN with other anomaly detection models.

![Anomaly Detection Pipeline](https://user-images.githubusercontent.com/68857931/207483255-5ca97661-be69-4451-b0e3-0385b0f91147.jpg)
