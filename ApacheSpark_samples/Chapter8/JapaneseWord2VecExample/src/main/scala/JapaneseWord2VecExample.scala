import java.io.StringReader
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.lucene.analysis.ja.JapaneseTokenizer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.ja.tokenattributes.BaseFormAttribute
import org.apache.lucene.analysis.ja.tokenattributes.PartOfSpeechAttribute
import org.apache.spark.mllib.linalg.Vectors

object JapaneseWord2VecExample {

  def tokenize(sentence: String) :Seq[String] = {
    // 分割して抽出した単語を入れる一時的な配列
    val word: scala.collection.mutable.ArrayBuffer[String] = new scala.collection.mutable.ArrayBuffer[String]()
    // 文書をトークナイズした結果を取り出します
    lazy val stream = new JapaneseTokenizer(new StringReader(sentence), null, false, JapaneseTokenizer.Mode.NORMAL)

    try {
      // トークンを取り出しながら、品詞をもとに選別します
      while(stream.incrementToken()) {
        var charAtt = stream.getAttribute(classOf[CharTermAttribute]).toString
        var bfAtt = stream.getAttribute(classOf[BaseFormAttribute]).getBaseForm
        // 品詞の1項目を取り出します
        var partOfSpeech = stream.getAttribute(classOf[PartOfSpeechAttribute]).getPartOfSpeech().split("-")(0)

        // 品詞と原型の内容によりマッチします。なお、動詞の場合は原型を登録します
        (partOfSpeech, bfAtt) match {
          case ("名詞", _)    => word += charAtt
          case ("動詞", null) => word += charAtt
          case ("動詞", baseForm)    => word += baseForm
          case (_, _)         =>
        }
      }
    } finally {
      stream.close
    }

    word.toSeq
  }

  def relationWords(w1: String, w2: String,target: String,model: Word2VecModel) :Array[(String, Double)] = {
    // BreezeというScalaの行列計算などを可能にするライブラリを利用します
    val b = breeze.linalg.Vector(model.getVectors(w1))
    val a = breeze.linalg.Vector(model.getVectors(w2))
    val c = breeze.linalg.Vector(model.getVectors(target))

    // v3に対して、v2 - v1の関係性を適用します
    val x = c + (a - b)

    // 関係性を加味したtに類似した単語を求めます
    model.findSynonyms(Vectors.dense(x.toArray.map(_.toDouble)),10)
  }

  def main(args: Array[String]) {

    if(args.length != 2) {
      System.err.println(
        "Usage: JapaneseWord2VecExample <inputdir> <outputdir>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("JapaneseWord2VecExample")
    val sc = new SparkContext(sparkConf)

    val input = sc.textFile(args(0)).map(line => tokenize(line))

    val word2vec = new Word2Vec()
    word2vec.setMinCount(3)
    word2vec.setVectorSize(30)
    val model = word2vec.fit(input)

    // ある単語に似た単語を10件抽出する(入力データに合わせて適宜変更してください)
//    val word = "単語A"
//    println("Input word: " + word)
//    model.findSynonyms(word,10).foreach{
//      w => println("word:" + w._1 + "  score:" + w._2)
//    }

    // 単語A,Bの関係を単語Cに適用する(入力データに合わせて適宜変更してください)
//    val wordA = "単語A"
//    val wordB = "単語B"
//    val wordC = "単語C"
//    println(wordC + " + ( " + wordA + " - " + wordB + " ) = ???")
//    relationWords(wordA,wordB,wordC,model).foreach{
//      w => println("word:" + w._1 + "  score:" + w._2)
//    }

    // モデルを保存する
    model.save(sc, args(1))

  }
}
