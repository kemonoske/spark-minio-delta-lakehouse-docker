FROM apache/hive:3.1.3

WORKDIR /opt

COPY hive-config/entrypoint.sh /entrypoint.sh

USER hive
EXPOSE 9083

ENTRYPOINT ["bash", "/entrypoint.sh"]