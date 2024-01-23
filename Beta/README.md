# Distributed System Lab Project Alpha 20231214 #
* RUN
```bash
sh start.sh dsys2313 2 2 NULL NULL
```
* BUG FIX
```bash
ps aux | grep kafka
kafka-server-stop.sh
kafka-topics.sh --delete --topic Topic1 --bootstrap-server localhost:9092
kafka-topics.sh --delete --topic Topic2 --bootstrap-server localhost:9092
```
