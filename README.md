# sparkSemi
====

Description
====

Sparkについて調べたことのメモ。

情報共有が目的。

2015/12/22時点でspark.ml.packageの一部についてまとめている。

mlパッケージとは
=====

正式にはspark.mlパッケージだけど面倒なのでここではmlパッケージと書きます。

DataFrameを入出力とする機械学習向けのハイレベルAPI。

DataFrameの性能が上がるとmlパッケージの性能も上がるということ。即ち、Project Tungstenの恩恵も受けられるということ。

Spark 1.6.0ではDataFrameのaggregateの性能が上がる。

MLlibには低レイヤーのmllibパッケージ、高レイヤーのmlパッケージの二つがあると考えるべき。

mlパッケージの方が柔軟性が高い。

mlパッケージにできてmllibパッケージにできないこと

* 処理のパイプライン記述
    * アイデアの元はscikit-leanとのこと。使ったことがある人はこの説明でだいたい分かる？（自分は使った事がないので分からなかった）
* どういうこと？
    * 生の入力データに対して複数の手法で特徴量を作る一連の流れをパイプラインとして記述できる
    *  入出力がDataFrameで統一されているので、処理の変更が容易。
* 使うときに意識することは？
    * DataFrameの特定のカラム名を指定してデータを加工（特徴量抽出）した結果は出力結果として指定したカラム名でDataFrameに追加される。
    * どのカラムに対するどんな処理の結果がどんなカラム名になっているかを覚えておく必要がある。
* MLBaseのコンポーネントの一部であるKeystoneMLが元で、そのコンセプトの一部がmlパッケージとしてMLlibに追加された背景がある。

mlパッケージのAPIの種類
=====
#### Transformers

DataFrameを変換して新しいDataFrameを作るもの。特徴変換やモデルがこれに該当する。transform()がある。

ML modelは特徴を持つDataFrameをpredictionsを持つDataFrameに変換するTransformer。

#### Estimators

Transformerを作るためのアルゴリズム。機械学習アルゴリズムとその周辺アルゴリズムが該当する。fit()がある。

機械学習のアルゴリズムはDataFrameを学習してmodelに変換する

※公式ドキュメントには他にも書かれているが、これらのAPIの一部に見えたので説明対象から除外。

利用例
===
テキストの分析で行う以下の一連の処理をパイプラインとして記述できる

* ドキュメントを単語に分割（Torkenizer）
* 単語を実数の特徴ベクトルに変換（HashingTF）
* 特徴ベクトルとラベルから学習して予測モデルを作成（Logistic Regression）

パイプラインの内部処理
===

TransformerもEstimatorもそれぞれ一つのstageに相当する。
* つまり、実行するたびにshuffleが実行されるということか？このstageがSparkのjobのstageと同一かは不明。

fit()を実行するとパイプラインの実行が開始される。

PipelineはEstimatorでありPipelineModelと呼ばれるTransformerを作る。
* EstimatorはTransformerを作るものであることを思い出そう。

PipelineはlinearだけではなくDAGも形成できる
* linearの場合は各stageは順番に、DAGの場合はトポロジカルオーダーで実行される（詳しい実装はコードを見ないと分からない）

 処理のパラメータの与え方
* TransformとEstimatorはパラメータを指定するための統合されたAPIとしてParamを持つ
    * 内部は"parameter"と"value"のペア
* fit()やtransform()にParamMapを与える
    * ParamMapは名前の通りParamのMap
    *  logistic regressionを例にすると、ParamMap(lr1.maxIter -> 10, lr2.maxIter -> 20)という形で与える。
																

mlパッケージのアルゴリズム
===
以下のカテゴリがある。
・特徴料抽出，特徴変換、特徴選択
・識別、回帰用の決定木
・アンサンブル
・Elastic Net正規化を使った線形メソッド
・多層パーセプトロン分類器

以降は上のカテゴリで使える機能を紹介していく。
紹介するアルゴリズムは今後リリースされるSpark 1.6.0のものを含む。

アルゴリズム：特徴料抽出，特徴変換、特徴選択
===

特徴抽出
---

#### TF-IDF（HashingTFとIDF）
##### TF
###### HashingTFのこと。種類はTransformer。
###### Term Frequency。即ち単語の出現銀度を示す特徴を作る。
##### IDF
##### 種類はEstimator。
###### Inverse Document Frequency。文章全体における単語の出現数をDFとして、DFの逆数を求める。
###### 即ち、文章中で出現頻度が少ない単語であるほど重要である＝大きな値になる。
##### 使い方
###### IDFで各文章における単語の重要度のモデルを作った後、TFで作ったベクトルを適用する。
###### 単語の重要度が似ている文章は内容が似ている文章である、といった文章の類似度の判定が可能。
																

#### Word2vec
##### 文章をベクトルに変換する。予め文章を単語に分割しておけば単語をベクトルに変換することもできる。
				似た文章は似たベクトルになるため、文章の類似性を判定することができる。
				使い方：
								TODO：サンプルコード


CountVectorizer
				CountVectorizer：Array[String]をテキストと見なして、テキスト中の単語と単語の出現頻度のベクトルを返す。
												パラメータとしてminDFを与えれば、出現頻度が一定以下の単語は結果から除外することができる。
																				CountVectorizerModelを生成するEstimator。
																				CountVectorizerModel：Transformer。ContVektorizerで生成したもの。これにDataFrameを与えると
																				使い方：
																								TODO：サンプルコード


=============特徴変換===================
Tokenizer
				Tokenizationer：文章を単語に分解する。
				RegexTokenizer：正規表現で一致する形で文章を分解する。デフォルトでは"¥s+"が指定されている。



StopWprdsRemover
				"a"や"the"、"with"など文章中に出現するものの全体からみてたいした意味を持たない単語をStopWordと呼ぶ。
				文章中からStopWordsを除外する。Tokenizerと組み合わせて使うことができる。
				Spark 1.6.0ではStopWordsとして320単語が登録されている。


n-gram
				n個の単語からなる言葉を返す。入力の単語数がn個未満の場合は何も返さない。
				検索のためのインデックスを作るときによく用いられる。
				英語でも日本語でも中国語でも言語を問わず適用できる一方で、意味ある単語を抜き出す精度は低い。


Binarizer
				数値を1.0と0.0の二値に分類する。与えた閾値を超えるものは1.0、超えないものは0.0と見なす。
　　　　　実行結果の例は以下の通り。
				+-----+-------+-----------------+
				|label|feature|binarized_feature|
				+-----+-------+-----------------+
				|    0|    0.1|              0.0|
				|    1|    0.8|              1.0|
				|    2|    0.2|              0.0|
				+-----+-------+-----------------+


PCA
				主成分分析。相関がある特徴どうしを相関がなくなるように変換する。
				5つの特徴を持つベクトルをPCAにかけることで3つの特徴を持つベクトルに次元削減することもできる。


PlynomicalExpansion
				多項式展開。3*2行列を3*9行列に拡張するといったことができる。


Discrete Cosine Transform(DCT)
				 離散コサイン変換。与えられた時系列の離散データを周波数成分に変換する。信号変換の一種。
				 この手法には標準的な手法が8種類、そのうちよく使われるのが4種類があるがSparkのmlパッケージが実装しているのはDCT-IIと呼ばれるもの。
				 

StringIndexer
				文字列が入っているカラムを数値に変換する。ラベルは0から始まる。出現数が多い文字列ほど小さな値のラベルになる。
				文字列のままではmlパッケージの機械学習アルゴリズムは学習できないため、数値データに変換するときに使う。
　　　　　実行例は以下の通り。
				+---+--------+-------------+
				| id|category|categoryIndex|
				+---+--------+-------------+
				|  0|       a|          0.0|
				|  1|       b|          2.0|
				|  2|       c|          1.0|
				|  3|       a|          0.0|
				|  4|       a|          0.0|
				|  5|       c|          1.0|
				+---+--------+-------------+


OneHotEncoder
				指定したカラムをバイナリベクトルに変換する。このベクトルは1の値を持つものは高々一カ所しかない。
				ロジスティック回帰のように連続値の特徴を期待するアルゴリズムがカテゴリ変数を扱えるようにすることができる。
				例えば、都道府県を特徴に変換するときにStringIndexerを使うと0〜47になるが、ロジスティック回帰は数値に大きさに意味を見いだそうとしてしまう。
　　　　　OneHotEncodingでは数値の差に意味が生じないように変換する。
　　　　　都道府県の場合、47次元のベクトルにして、どれか一つに1をつけることで数値の大きさを無視できる値に変換できる。
　　　　　東京：[0, 0, 0, 0, … , 0]
　　　　　大阪：[1, 0, 0, 0, … , 0]
　　　　　京都：[0, 1, 0, 0, … , 0]

VectorIndexer
				ベクトルを与えた上で最大カテゴリ数を指定すると、ベクトルの中からカテゴリになることができる特徴を自動で見つけ、特徴にラベル付けしてくれる。
				決定木やTree Ensembleにカテゴリカルな特徴を扱わせるときに使うことができる。
				パフォーマンスも改善される。
				3次元のベクトルを3つ持つデータに最大の次元数を3を指定してモデルを作った後transformで変換すると以下のような結果になる。
　　　　　featuresの一次元目のデータ[0.0, -1.0, 14.0]が3つのカテゴリとして認識されていることが分かる。
　　　　　二次元目のデータは二つ目と三つ目が2.0と同じあたいであり、変換後も同じカテゴリになっている。
				+-------------------+-----------------+
				|           features|          indexed|
				+-------------------+-----------------+
				| [0.0,1.0,-2.0,3.0]|[0.0,0.0,1.0,1.0]|
				|[-1.0,2.0,4.0,-7.0]|[1.0,1.0,2.0,0.0]|
				|[14.0,2.0,-5.0,3.0]|[2.0,1.0,0.0,1.0]|
				+-------------------+-----------------+


Normalizer
　　　　ベクトルをうけとり正規化する。与えられたパラメータpを受け取り、pノルムを使い正規化する。
　　　　デフォルトではp=2でユークリッド距離。
　　　　Transformerの一つ。


StandardScaler
　　　　特徴を標準偏差1、平均が0になるように正規化することができる。
　　　　パラメータとしてwithStdがtrueなら標準偏差を1にする。
　　　　withMeanがtrueならスケールする前にデータを平均に集める。denseなVectorを出力する。sparseな入力には機能しない（将来的に対応予定）。


MinmaxScaler
　　　　指定したMinとMaxにスケールするようにデータを正規化する。デフォルトで[0, 1)
　　　　※　具体的に実行してみたところ、確認できる範囲の数値は全て0.5に変換されていた。
				+-----+--------------------+--------------------+
				|label|            features|      scaledFeatures|
				+-----+--------------------+--------------------+
				|  0.0|(692,[127,128,129...|[0.5,0.5,0.5,0.5,...|
				|  1.0|(692,[99,100,101,...|[0.5,0.5,0.5,0.5,...|
				|  0.0|(692,[124,125,126...|[0.5,0.5,0.5,0.5,...|
				|  0.0|(692,[153,154,155...|[0.5,0.5,0.5,0.5,...|
				|  1.0|(692,[125,126,153...|[0.5,0.5,0.5,0.5,...|
				|  1.0|(692,[125,126,127...|[0.5,0.5,0.5,0.5,...|
				|  0.0|(692,[126,127,128...|[0.5,0.5,0.5,0.5,...|
				|  0.0|(692,[152,153,154...|[0.5,0.5,0.5,0.5,...|
				|  1.0|(692,[127,128,129...|[0.5,0.5,0.5,0.5,...|
				|  1.0|(692,[123,124,125...|[0.5,0.5,0.5,0.5,...|
				|  0.0|(692,[123,124,125...|[0.5,0.5,0.5,0.5,...|
				|  1.0|(692,[125,126,153...|[0.5,0.5,0.5,0.5,...|
				|  1.0|(692,[124,125,126...|[0.5,0.5,0.5,0.5,...|
				|  1.0|(692,[100,101,102...|[0.5,0.5,0.5,0.5,...|
				+-----+--------------------+--------------------+　　　　

Bucketizer
　　　　連続値の特徴をバケットの特徴に変換する。バケットはユーザが指定するsplitsパラメータによって決まる。
　　　　splits：バケットに変換する範囲である[x, y)を指定して、連続値の区切りを決める。
　　　　　　　　 パラメータの例：val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)
　　　　この時の実行例
				+--------+----------------+
				|features|bucketedFeatures|
				+--------+----------------+
				|   -10.0|             0.0|
				|   -0.51|             0.0|
				|    -0.5|             1.0|
				|    -0.3|             1.0|
				|     0.0|             2.0|
				|     0.2|             2.0|
				|     0.5|             3.0|
				|     1.0|             3.0|
				+--------+----------------+

　　　　　splitsとしてArray(Double.NegativeInfinity, 0.5, Double.PositiveInfinity)を指定するとBinarizerとして使うことができる。


ElementwiseProduct
　　　　ベクトルの各要素ごとにかけ算することができる。重み計算に使える。
　　　　(0.0, 1.0, 2.0)をかけ算したときの実行例；
							+---+-------------+-----------------+
							| id|       vector|transformedVector|
							+---+-------------+-----------------+
							|  a|[1.0,2.0,3.0]|    [0.0,2.0,6.0]|
							|  b|[4.0,5.0,6.0]|   [0.0,5.0,12.0]|
							+---+-------------+-----------------+


VectorAssembler
　　　　複数のカラムを一つのカラムにまとめる。生の特徴と別の特徴をTransformerを使って変換して得られた新しい特徴をまとめるときに便利。
　　　　以下のようなユーザのwebアクセスとクリック情報があるとする。
				 id | hour | mobile | userFeatures     | clicked
				 ----|------|--------|------------------|---------
				  0  | 18   | 1.0    | [0.0, 10.0, 0.5] | 1.0
　　　　ここで、クリックするかどうかを判定するモデルをつくるために情報を一つにまとめたいときにVectorAssemblerが役立つ。
				 id | hour | mobile | userFeatures     | clicked | features
				 ----|------|--------|------------------|---------|-----------------------------
				  0  | 18   | 1.0    | [0.0, 10.0, 0.5] | 1.0     | [18.0, 1.0, 0.0, 10.0, 0.5]



=============特徴選択===================

VectorSlicer
　　　　VectorAssemblerの逆の機能。一つの特徴ベクトルから指定したインデックスの特徴のみを取り出す。
　　　　K番目とJ番目の特徴だけが欲しい、というときに使える。

RFormula
　　　　RFormulaに記述で指定されたカラムを選択して、ベクトルを持つカラムを作る。
　　　　以下のようなデータがあるとする。
				id | country | hour | clicked
				---|---------|------|---------
				 7 | "US"    | 18   | 1.0
				  8 | "CA"    | 12   | 0.0
					 9 | "NZ"    | 15   | 0.0

　　　　　clickedを予測するためにcountryとhourを特徴として使いたいとすると、"clicked ~ country + hour"を与える。
　　　　　RFormulaはcountryの文字列をone-hot encodingし、hourをdoubleにキャストして新しい特徴を作り出す。
																						 id | country | hour | clicked | features         | label
																						 ---|---------|------|---------|------------------|-------
																						  7 | "US"    | 18   | 1.0     | [0.0, 0.0, 18.0] | 1.0
																							 8 | "CA"    | 12   | 0.0     | [0.0, 1.0, 12.0] | 0.0
																							  9 | "NZ"    | 15   | 0.0     | [1.0, 0.0, 15.0] | 0.0


ChiSQSelector(Spark 1.6.0から)
　　　　ラベルが付けられているカテゴリカル変数に対して実行できる。χ二乗検定。
　　　　

【アルゴリズム：識別、回帰用の決定木】



【アルゴリズム：アンサンブル】


【アルゴリズム：Elastic Net正規化を使った線形メソッド】



【アルゴリズム：多層パーセプトロン分類器】







【実践編】
単語の特徴量の話にチャレンジしてみる。
目的は、本を分析して、誰のセリフかをあてること。
登場人物 + 地の文が正解例。
日本語の場合、形態素解析ライブラリが必要になるのでSparkの特徴調抽出アルゴリズムでもそこそこ分析できると思われる英語の本から選ぶ。
今回は登場便物が比較的少なく、かつ特定のキャラクターのセリフが極端に少なくて学習が難しくない題材を選びたい。
登場人物がすくなく、英語で、かつ答え合わせがしやすいものとしてグリム童話の中から「ヘンゼルとグレーテル」を選んだ。
※ 初めはイソップ物語から探していたものの、一つ一つの物語の文章量が少ないので諦めた

データは"Project Futenberg"というサイトからダウンロード。
http://www.gutenberg.org/wiki/Main_Page
著作権が切れた作品のデータを公開している。
「ヘンゼルとグレーテル」を含むグリム童話のプレーンテキストは以下からダウンロードできる。
http://www.gutenberg.org/cache/epub/2591/pg2591.txt

mlパッケージのパラメータはドキュメントでは紹介されていないものもあるため、API集を確認したりソースコードを読まないと全てのパラメータを知ることはできない。
API集ではパラメータに説明がないためソースを読むかパラメータの名前で推測するしかない。

【実行したコードと結果】
scala> val data = sc.textFile("/data/hanze_and_gretel.txt").toDF("text").cache
data: org.apache.spark.sql.DataFrame = [text: string]

scala> data.show
+--------------------+                                                          
|                text|
+--------------------+
|   HANSEL AND GRETEL|
|                    |
|Hard by a great f...|
|The boy was calle...|
|He had little to ...|
|Now when he thoug...|
|'What is to becom...|
|'I'll tell you wh...|
|answered the woman, |
|'early tomorrow m...|
|of them one more ...|
|        'No, wife,' |
|      said the man, |
|'I will not do th...|
|     'O, you fool!' |
|          said she, |
|'then we must all...|
| and she left him...|
|'But I feel very ...|
|       said the man.|
+--------------------+
only showing top 20 rows

// 単語に分割
scala> import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.Tokenizer

scala> val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
tokenizer: org.apache.spark.ml.feature.Tokenizer = tok_d2be1f0788bd

scala> val tokenized = tokenizer.transform(data)
tokenized: org.apache.spark.sql.DataFrame = [text: string, words: array<string>]

// StopWorkdsの除去
scala> import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.StopWordsRemover

scala> val remover = new StopWordsRemover().setInputCol("words").setOutputCol("removedSW")
remover: org.apache.spark.ml.feature.StopWordsRemover = stopWords_397a5af42ccf

scala> val removedSW = remover.transform(tokenized)
removedSW: org.apache.spark.sql.DataFrame = [text: string, words: array<string>, removedSW: array<string>]

scala> removedSW.show
+--------------------+--------------------+--------------------+
|                text|               words|           removedSW|
+--------------------+--------------------+--------------------+
|   HANSEL AND GRETEL|[hansel, and, gre...|    [hansel, gretel]|
|                    |                  []|                  []|
|Hard by a great f...|[hard, by, a, gre...|[hard, great, for...|
|The boy was calle...|[the, boy, was, c...|[boy, called, han...|
|He had little to ...|[he, had, little,...|[little, bite, br...|
|Now when he thoug...|[now, when, he, t...|[thought, night, ...|
|'What is to becom...|['what, is, to, b...|['what, us?, feed...|
|'I'll tell you wh...|['i'll, tell, you...|['i'll, tell, wha...|
|answered the woman, |[answered, the, w...|  [answered, woman,]|
|'early tomorrow m...|['early, tomorrow...|['early, tomorrow...|
|of them one more ...|[of, them, one, m...|[piece, bread,, w...|
|        'No, wife,' |    [, 'no,, wife,']|    [, 'no,, wife,']|
|      said the man, |   [said, the, man,]|        [said, man,]|
|'I will not do th...|['i, will, not, d...|['i, that;, bear,...|
|     'O, you fool!' |  ['o,, you, fool!']|       ['o,, fool!']|
|          said she, |        [said, she,]|        [said, she,]|
|'then we must all...|['then, we, must,...|['then, die, hung...|
| and she left him...|[, and, she, left...|[, left, peace, c...|
|'But I feel very ...|['but, i, feel, v...|['but, feel, sorr...|
|       said the man.|   [said, the, man.]|        [said, man.]|
+--------------------+--------------------+--------------------+
only showing top 20 rows

// word2vecで単語をベクトル化
scala> import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.feature.Word2Vec

// setVectorSizeで10に指定しているのは、ベクトルサイズが登場人物よりも多くないと正しい学習ができないと考えた結果、
// 物語はヘンゼルとグレーテル、魔女の3人とパンを食べてしまう動物だけだったが、原著には他にもキャラクターがいる可能性を考慮
scala> val word2Vec = new Word2Vec().setInputCol("removedSW").setOutputCol("word2vector").setVectorSize(10).setMinCount(0)
word2Vec: org.apache.spark.ml.feature.Word2Vec = w2v_62a9c29eb629

scala> val model = word2Vec.fit(removedSW)
15/12/16 22:20:26 WARN BLAS: Failed to load implementation from: com.github.fommil.netlib.NativeSystemBLAS
15/12/16 22:20:26 WARN BLAS: Failed to load implementation from: com.github.fommil.netlib.NativeRefBLAS
model: org.apache.spark.ml.feature.Word2VecModel = w2v_62a9c29eb629

scala> val result  = model.transform(removedSW)
result: org.apache.spark.sql.DataFrame = [text: string, words: array<string>, removedSW: array<string>, word2vector: vector]

scala> result.show
+--------------------+--------------------+--------------------+--------------------+
|                text|               words|           removedSW|         word2vector|
+--------------------+--------------------+--------------------+--------------------+
|   HANSEL AND GRETEL|[hansel, and, gre...|    [hansel, gretel]|[-0.0030053327791...|
|                    |                  []|                  []|[0.03200648725032...|
|Hard by a great f...|[hard, by, a, gre...|[hard, great, for...|[0.01893300696974...|
|The boy was calle...|[the, boy, was, c...|[boy, called, han...|[0.00844271602109...|
|He had little to ...|[he, had, little,...|[little, bite, br...|[-0.0049137868071...|
|Now when he thoug...|[now, when, he, t...|[thought, night, ...|[-0.0105014385771...|
|'What is to becom...|['what, is, to, b...|['what, us?, feed...|[-0.0033563544441...|
|'I'll tell you wh...|['i'll, tell, you...|['i'll, tell, wha...|[0.02176030795089...|
|answered the woman, |[answered, the, w...|  [answered, woman,]|[2.57648178376257...|
|'early tomorrow m...|['early, tomorrow...|['early, tomorrow...|[0.00338082341477...|
|of them one more ...|[of, them, one, m...|[piece, bread,, w...|[0.01291667534546...|
|        'No, wife,' |    [, 'no,, wife,']|    [, 'no,, wife,']|[0.03642651314536...|
|      said the man, |   [said, the, man,]|        [said, man,]|[-0.0270225470885...|
|'I will not do th...|['i, will, not, d...|['i, that;, bear,...|[-0.0041762024629...|
|     'O, you fool!' |  ['o,, you, fool!']|       ['o,, fool!']|[0.00498255528509...|
|          said she, |        [said, she,]|        [said, she,]|[-0.0340098459273...|
|'then we must all...|['then, we, must,...|['then, die, hung...|[-0.0038514856714...|
| and she left him...|[, and, she, left...|[, left, peace, c...|[-0.0114966095861...|
|'But I feel very ...|['but, i, feel, v...|['but, feel, sorr...|[-0.0042414270962...|
|       said the man.|   [said, the, man.]|        [said, man.]|[7.78345391154289...|
+--------------------+--------------------+--------------------+--------------------+
only showing top 20 rows

// 登場人物を識別してみる
// まずは主要キャラクターの3人。
// Kmeansでクラスタリング
// spark.mlにKmeansが加わったのはSpark 1.5.0から。
// Spark 1.4.x以下を使っている人はDataFrameをRDDに変換してからspark.mllib.kmeansを使いましょう。
scala> import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.clustering.KMeans

// 入出力のカラムを指定するAPIがこれまでと違うことに注意
scala> val kmeans = new KMeans().setFeaturesCol("word2vector").setPredictionCol("label").setK(3)
kmeans: org.apache.spark.ml.clustering.KMeans = kmeans_2164e375bb04

scala> val kmeansModel = kmeans.fit(result)
15/12/16 22:35:26 WARN KMeans: The input data is not directly cached, which may hurt performance if its parent RDDs are also uncached.
15/12/16 22:35:29 WARN KMeans: The input data was not directly cached, which may hurt performance if its parent RDDs are also uncached.
kmeansModel: org.apache.spark.ml.clustering.KMeansModel = kmeans_2164e375bb04

scala> val labeled = kmeansModel.transform(result)
labeled: org.apache.spark.sql.DataFrame = [text: string, words: array<string>, removedSW: array<string>, word2vector: vector, label: int]

scala> labeled.show
+--------------------+--------------------+--------------------+--------------------+-----+
|                text|               words|           removedSW|         word2vector|label|
+--------------------+--------------------+--------------------+--------------------+-----+
|   HANSEL AND GRETEL|[hansel, and, gre...|    [hansel, gretel]|[-0.0030053327791...|    0|
|                    |                  []|                  []|[0.03200648725032...|    1|
|Hard by a great f...|[hard, by, a, gre...|[hard, great, for...|[0.01893300696974...|    2|
|The boy was calle...|[the, boy, was, c...|[boy, called, han...|[0.00844271602109...|    2|
|He had little to ...|[he, had, little,...|[little, bite, br...|[-0.0049137868071...|    2|
|Now when he thoug...|[now, when, he, t...|[thought, night, ...|[-0.0105014385771...|    0|
|'What is to becom...|['what, is, to, b...|['what, us?, feed...|[-0.0033563544441...|    2|
|'I'll tell you wh...|['i'll, tell, you...|['i'll, tell, wha...|[0.02176030795089...|    2|
|answered the woman, |[answered, the, w...|  [answered, woman,]|[2.57648178376257...|    2|
|'early tomorrow m...|['early, tomorrow...|['early, tomorrow...|[0.00338082341477...|    2|
|of them one more ...|[of, them, one, m...|[piece, bread,, w...|[0.01291667534546...|    2|
|        'No, wife,' |    [, 'no,, wife,']|    [, 'no,, wife,']|[0.03642651314536...|    2|
|      said the man, |   [said, the, man,]|        [said, man,]|[-0.0270225470885...|    0|
|'I will not do th...|['i, will, not, d...|['i, that;, bear,...|[-0.0041762024629...|    2|
|     'O, you fool!' |  ['o,, you, fool!']|       ['o,, fool!']|[0.00498255528509...|    2|
|          said she, |        [said, she,]|        [said, she,]|[-0.0340098459273...|    0|
|'then we must all...|['then, we, must,...|['then, die, hung...|[-0.0038514856714...|    2|
| and she left him...|[, and, she, left...|[, left, peace, c...|[-0.0114966095861...|    2|
|'But I feel very ...|['but, i, feel, v...|['but, feel, sorr...|[-0.0042414270962...|    2|
|       said the man.|   [said, the, man.]|        [said, man.]|[7.78345391154289...|    0|
+--------------------+--------------------+--------------------+--------------------+-----+
only showing top 20 rows

// データを見やすくして答えを確認してみる
scala> val character = labeled.select("text", "label").limit(20)
character: org.apache.spark.sql.DataFrame = [text: string, label: int]

scala> character.collect.foreach(println)
[HANSEL AND GRETEL,0]
[,1]
[Hard by a great forest dwelt a poor wood-cutter with his wife and his two children.,2]
[The boy was called Hansel and the girl Gretel. ,2]
[He had little to bite and to break, and once when great dearth fell on the land, he could no longer procure even daily bread. ,2]
[Now when he thought over this by night in his bed, and tossed about in his anxiety, he groaned and said to his wife: ,0]
['What is to become of us? How are we to feed our poor children, when we no longer have anything even for ourselves?' ,2]
['I'll tell you what, husband,' ,2]
[answered the woman, ,2]
['early tomorrow morning we will take the children out into the forest to where it is the thickest; there we will light a fire for them, and give each,2]
[of them one more piece of bread, and then we will go to our work and leave them alone. They will not find the way home again, and we shall be rid of them.',2]
[ 'No, wife,' ,2]
[said the man, ,0]
['I will not do that; how can I bear to leave my children alone in the forest?--the wild animals would soon come and tear them to pieces.' ,2]
['O, you fool!' ,2]
[said she, ,0]
['then we must all four die of hunger, you may as well plane the planks for our coffins,',2]
[ and she left him no peace until he consented. ,2]
['But I feel very sorry for the poor children, all the same,' ,2]
[said the man.,0]

// ほとんどのテキストが[2]にラベル付けされている。
// 各ラベルの数を確認してみる。
scala> labeled.select("label").map(line => (line, 1)).reduceByKey(_ + _)
res30: org.apache.spark.rdd.RDD[(org.apache.spark.sql.Row, Int)] = ShuffledRDD[171] at reduceByKey at <console>:48

scala> res30.collect.foreach(println)
([0],34)                                                                        
([2],161)
([1],21)

// 2が地文だとすると、[0]と[1]はなんだろう、と思ったところで登場人物はヘンゼルとグレーテルと魔女と地文の4つにするべきだったと気づく。
// kの値を4でやり直す。
scala> val kmeans4 = kmeans.setK(4)
kmeans4: kmeans.type = kmeans_2164e375bb04

scala> val labeled = kmeansModel.transform(result)
labeled: org.apache.spark.sql.DataFrame = [text: string, words: array<string>, removedSW: array<string>, word2vector: vector, label: int]

scala> labeled.select("text", "label").limit(20).collect.foreach(println)
[HANSEL AND GRETEL,2]
[,1]
[Hard by a great forest dwelt a poor wood-cutter with his wife and his two children.,3]
[The boy was called Hansel and the girl Gretel. ,3]
[He had little to bite and to break, and once when great dearth fell on the land, he could no longer procure even daily bread. ,0]
[Now when he thought over this by night in his bed, and tossed about in his anxiety, he groaned and said to his wife: ,0]
['What is to become of us? How are we to feed our poor children, when we no longer have anything even for ourselves?' ,0]
['I'll tell you what, husband,' ,0]
[answered the woman, ,3]
['early tomorrow morning we will take the children out into the forest to where it is the thickest; there we will light a fire for them, and give each,3]
[of them one more piece of bread, and then we will go to our work and leave them alone. They will not find the way home again, and we shall be rid of them.',0]
[ 'No, wife,' ,3]
[said the man, ,2]
['I will not do that; how can I bear to leave my children alone in the forest?--the wild animals would soon come and tear them to pieces.' ,0]
['O, you fool!' ,0]
[said she, ,2]
['then we must all four die of hunger, you may as well plane the planks for our coffins,',0]
[ and she left him no peace until he consented. ,3]
['But I feel very sorry for the poor children, all the same,' ,0]
[said the man.,2]

scala> labeled.select("label").map(line => (line, 1)).reduceByKey(_ + _).collect.foreach(println)
([3],52)
([0],127)
([2],16)
([1],21)

// [0]は地文、[3]はおしゃべりさん、[1]と[2]は他の二人。[3]は魔女？
scala> labeled.where("label = 0").show
+--------------------+--------------------+--------------------+--------------------+-----+
|                text|               words|           removedSW|         word2vector|label|
+--------------------+--------------------+--------------------+--------------------+-----+
|He had little to ...|[he, had, little,...|[little, bite, br...|[-0.0049137868071...|    0|
|Now when he thoug...|[now, when, he, t...|[thought, night, ...|[-0.0105014385771...|    0|
|'What is to becom...|['what, is, to, b...|['what, us?, feed...|[-0.0033563544441...|    0|
|'I'll tell you wh...|['i'll, tell, you...|['i'll, tell, wha...|[0.02176030795089...|    0|
|of them one more ...|[of, them, one, m...|[piece, bread,, w...|[0.01291667534546...|    0|
|'I will not do th...|['i, will, not, d...|['i, that;, bear,...|[-0.0041762024629...|    0|
|     'O, you fool!' |  ['o,, you, fool!']|       ['o,, fool!']|[0.00498255528509...|    0|
|'then we must all...|['then, we, must,...|['then, die, hung...|[-0.0038514856714...|    0|
|'But I feel very ...|['but, i, feel, v...|['but, feel, sorr...|[-0.0042414270962...|    0|
|The two children ...|[the, two, childr...|[children, able, ...|[-0.0144949704408...|    0|
|'Now all is over ...|['now, all, is, o...|        ['now, us.']|[-0.0154860946349...|    0|
|'do not distress ...|['do, not, distre...|['do, distress, y...|[-0.0084313481513...|    0|
|And when the old ...|[and, when, the, ...|[old, folks, fall...|[-0.0010562608233...|    0|
|'Be comforted, de...|['be, comforted,,...|['be, comforted,,...|[-0.0120439829654...|    0|
|and he lay down a...|[and, he, lay, do...|         [lay, bed.]|[-0.0278038121759...|    0|
|When day dawned, ...|[when, day, dawne...|[day, dawned,, su...|[-0.0155352879729...|    0|
|'Get up, you slug...|['get, up,, you, ...|['get, up,, slugg...|[-0.0033770013999...|    0|
|'There is somethi...|['there, is, some...|['there, dinner,,...|[-0.0035830006003...|    0|
|Then they all set...|[then, they, all,...| [set, way, forest.]|[0.00553607568144...|    0|
|When they had wal...|[when, they, had,...|[walked, short, t...|[0.00502596588598...|    0|
+--------------------+--------------------+--------------------+--------------------+-----+


// [0]は地文ですらない。
// 考えられるのは、
//   (1) word2vecとkmeansでは登場人物を当てることはできない
//   (2) "!"や"'"などの記号くらいは除去しないとそもそもまともな識別ができない。
// (2)の可能性を検証する。
// removedSWにUDFを適用して不要な記号を消すこともできる。
// しかし、今回は機械学習の繰り返しとspark.mlのパイプラインを生かすことにする。




