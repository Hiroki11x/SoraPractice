# Spark Streamingのサンプルアプリケーション（2）

## JAR作成方法

SBTもしくはActivatorを利用します。

SBTを利用する場合は以下のコマンドを実行します。

```
$ sbt clean assembly
```

Activatorを利用する場合は以下のコマンドを実行します。

```
$ activator clean assembly
```

以下のJARファイルが生成されます。

target/scala-2.10/streaming-second-assembly-0.1.0-SNAPSHOT.jar

## 実行方法

```
$ spark-submit --master local[4] --class StreamingSecond target/scala-2.10/streaming-second-assembly-0.1.0-SNAPSHOT.jar <URL>
```

引数はHDFS上の監視対象ディレクトリを表すURLです。
