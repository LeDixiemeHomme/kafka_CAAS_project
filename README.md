## Lancer le .jar

Il faut qu'un fichier "config.properties" soit présent au même niveau que le .jar.
Pour lancer le producer "java -jar kafka_CAAS_project-1.0-SNAPSHOT.jar produce"
Pour lancer le consumer "java -jar kafka_CAAS_project-1.0-SNAPSHOT.jar consume"

## Créer un fichier config.properties

Il faut que deux propriétés soient présentes :
- brokers : est l'adresse et le port des brokers.

Exemple : brokers=localhost:9092
- topicName : est le nom du topic sur lequel vous voulez intéragir. 

Exemple : topicName=aTopic

