import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ DateType, FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

object Main extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("BankProject")
    .getOrCreate()

  spark.sparkContext.setLogLevel("Error")
  spark.conf.set("spark.sql.shuffle.partitions", "20")

  val accountSchema = StructType(Array(
    StructField("AccountID", IntegerType, nullable = true),
    StructField("AccountNum", StringType, nullable = true),
    StructField("ClientID", IntegerType, nullable = true),
    StructField("DateOpen", DateType, nullable = true)
  ))
  val clientsSchema = StructType(Array(
    StructField("ClientId", IntegerType, nullable = true),
    StructField("ClientName", StringType, nullable = true),
    StructField("Type", StringType, nullable = true),
    StructField("Form", StringType, nullable = true),
    StructField("RegisterDate", DateType, nullable = true)
  ))
  val operationSchema = StructType(Array(
    StructField("AccountDB", IntegerType, nullable = true),
    StructField("AccountCR", IntegerType, nullable = true),
    StructField("DateOp", DateType, nullable = true),
    StructField("Amount", FloatType, nullable = true),
    StructField("Currency", StringType, nullable = true),
    StructField("Comment", StringType, nullable = true)
  ))
  val rateSchema = StructType(Array(
    StructField("Currency", StringType, nullable = true),
    StructField("Rate", StringType, nullable = true),
    StructField("RateDate", DateType, nullable = true)
  ))

  val accountsDf = spark.read
    .format("csv")
    .option("header", "true")
    .csv("src/main/scala/Account.csv")
  val clientsDf = spark.read
    .format("csv")
    .option("header", "true")
    .csv("src/main/scala/Сlients.csv")
  val operationDf = spark.read
    .format("csv")
    .option("header", "true")
    .csv("src/main/scala/Operation.csv")
  val rateDf = spark.read
    .format("csv")
    .option("header", "true")
    .csv("src/main/scala/rate.csv")


  val temp = accountsDf
    .join(clientsDf, accountsDf("ClientId") === clientsDf("ClientsId"))
    .drop(clientsDf("ClientId"))
    .persist(StorageLevel.MEMORY_ONLY_SER)

  val temp1 = rateDf.select(
    col("RateDate").as("DateRate"),
    col("Currency").as("CurRate"),
    col("Rate"))
    .where(!col("CurRate").like("RUB"))


  var daterates = operationDf.select(col("DateOp").as("Date")).distinct()
  daterates = daterates.crossJoin(temp1.select(col("CurRate").as("Currency")).distinct()
  )

  daterates = daterates
    .join(temp1, daterates("Date") === temp1("DateRate")
      && daterates("Currency") === ("CurRate"), "left")
    .drop("DateRate")
    .drop("CurRate")

  val show = Window.partitionBy("Currency").orderBy(desc("Rate"), asc("Date"))

  val result = operationDf
    .join(temp.select(
      col("AccountId").alias("AccDB"),
      col("AccountNum").alias("NumDB"),
      col("Type").alias("TypeDB"),
      col("ClientID").alias("ClientDB")
    ), col("AccDB") === col("AccountDB") || col("AccDB") === col("AccountCR"), "left")
    .join(temp.select(
      col("AccountId").alias("AccCR"),
      col("AccountNum").alias("NumCR"),
      col("Type").alias("TypeCR"),
      col("ClientID").alias("ClientCR")),
      col("AccCR") === col("AccountCR"), "left")
    .join(daterates, col("DateOp") === col("Date") && operationDf("Currency") === daterates("Currency"), "left")
    .drop(daterates("Currency"))
    .drop(daterates("Date"))

  val arrAuto = raw"%а/м%, %а\м%, %автомобиль %, %автомобили %, %транспорт%, %трансп%средс%, %легков%, %тягач%, %вин%, %vin%,%viн:%, %fоrd%, %форд%,%кiа%, %кия%, %киа%%мiтsuвisнi%, %мицубиси%, %нissан%, %ниссан%, %sсанiа%, %вмw%, %бмв%, %аudi%, %ауди%, %jеер%, %джип%, %vоlvо%, %вольво%, %тоyота%, %тойота%, %тоиота%, %нyuнdаi%, %хендай%, %rенаulт%, %рено%, %реugеот%, %пежо%, %lаdа%, %лада%, %dатsuн%, %додж%, %меrсеdеs%, %мерседес%, %vоlкswаgен%, %фольксваген%, %sкоdа%, %шкода%, %самосвал%, %rover%, %ровер%"
  val arrEat = raw"% сою%, %соя%, %зерно%, %кукуруз%, %масло%, %молок%, %молоч%, %мясн%, %мясо%, %овощ%, %подсолн%, %пшениц%, %рис%, %с/х%прод%, %с/х%товар%, %с\х%прод%, %с\х%товар%, %сахар%, %сельск%прод%, %сельск%товар%, %сельхоз%прод%, %сельхоз%товар%, %семен%, %семечк%, %сено%, %соев%, %фрукт%, %яиц%, %ячмен%, %картоф%, %томат%, %говя%, %свин%, %курин%, %куриц%, %рыб%, %алко%, %чаи%, %кофе%, %чипс%, %напит%, %бакале%, %конфет%, %колбас%, %морож%, %с/м%, %с\м%, %консерв%, %пищев%, %питан%, %сыр%, %макарон%, %лосос%, %треск%, %саир%, % филе%, % хек%, %хлеб%, %какао%, %кондитер%, %пиво%, %ликер%"

  val condAuto = arrAuto.map(condition => !col("Comment").like(raw"$condition")).reduce(_ || _)
  val condEat = arrEat.map(condition => !col("Comment").like(raw"$condition")).reduce(_ || _)

  val convertRate = when(col("Currency").like("RUB"), col("Amount")).otherwise(col("Amount") * col("Rate"))

  /*
AccountId
ИД счета

ClientId
Ид клиента счета

PaymentAmt
Сумма операций по счету, где счет клиента указан в дебете проводки

EnrollementAmt
Сумма операций по счету, где счет клиента указан в  кредите проводки

TaxAmt
Сумму операций, где счет клиента указан в дебете, и счет кредита 40702

ClearAmt
Сумма операций, где счет клиента указан в кредите, и счет дебета 40802

CarsAmt
Сумма операций, где счет клиента указан в дебете проводки и назначение платежа не содержит слов по маскам Списка 1

FoodAmt
Сумма операций, где счет клиента указан в кредите проводки и назначение платежа содержит слова по Маскам Списка 2

FLAmt
Сумма операций с физ. лицами. Счет клиента указан в дебете проводки, а клиент в кредите проводки – ФЛ.

CutoffDt
Дата операции;
*/
  result.groupBy(col("AccDB").as("AccountID"), col("DateOp").as("CutoffDate"))
    .agg(
      round(sum(when(col("AccDB") === col("AccountDB"), convertRate))).as("PaymentAmt"),
      round(sum(when(col("AccDB") === col("AccountCR"), convertRate))).as("EnrollmentAmt"),
      round(sum(when(col("AccDB") === col("AccountDB") && col("NumCr").startsWith("40702"), convertRate))).as("TaxAmt"),
      round(sum(when(col("AccDB") === col("AccountCR") && col("NumDb").startsWith("40802"), convertRate))).as("ClearAmt"),
      round(sum(when(col("AccDB") === col("AccountDB") && condAuto, convertRate))).as("AutoAmt"),
      round(sum(when(col("AccDB") === col("AccountCR") && condEat, convertRate))).as("EatAmt"),
      round(sum(when(col("AccDB") === col("AccountDB") && col("TypeCR") === "0", convertRate))).as("FLAmt")
    ).orderBy("AccDb", "DateOp").show()

  Thread.sleep(Int.MaxValue)
}

