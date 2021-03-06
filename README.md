# Описание #

Улучшенная версия решения [задачи](https://github.com/joesephz/interns-test/wiki), где, в отличии от [предыдущего решения](https://github.com/xxlabaza/csv-merger-java), у нас появилось неможко оперативы (!!!) и можем даже позволить себе создавать и хранить структуры данных!

Смысл решения довольно простой:

Прочитали, скажем, 100 строк из **первого файла**, распарсили их в мультимапу, например вида **Map\<String, List\<String\>\>**. Тоже самое сделали со **вторым файлом**. Сравнили мапы, если есть пересекающиеся ключи - пишем сразу в **result.csv** appendo'ом.

Получившуюся мультимапу **второго файла** сериализуем на диск и читаем следующие 100 строк из **второго файла**, так же сравниваем с всё ещё **первой соткой из первого**, есть совпадения - пишем в **result.csv**. Опять мультимапу **второго файла** сериализуем в файл. 

И так пока не кончится **второй файл** и у нас не образуется на диске кулёк сериализованных мультимап в виде файлов. Затем читаем следующую сотку из **первого файла** и сравниваем её, по очерёдно, с каждым куском сериализованных данных из второго файла.

```bash
$> java -jar csv-merger-1.0.0.jar -i ~/first.csv ~/second.csv -o ~/result.csv
$> ls ./tmp   # аутпут представлен при чтении по 2 строки за раз
100-second.csv 150-second.csv 199-second.csv 50-second.csv
$> cat result.csv 
000000011,EKH3-5IDH-DNUK,NW8U-C0IQ-WWKV
000000001,4DG1-WUX0-F16B,RLT0-DFHH-1U5U
000000015,UJ8E-G1IV-U9PS,Q0Z1-4JN9-PS7U
000000012,DS9X-1N3P-KIAW,ZPMK-N3BU-C33D
000000012,DS9X-1N3P-KIAW,M3LF-XPWB-8NI6
```
