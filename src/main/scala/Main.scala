import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

object Main extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("BankProject")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
 // spark.conf.set("spark.sql.shuffle.partitions", "20")

  val accountSchema = StructType(Array(
    StructField("AccountID", IntegerType),
    StructField("AccountNum", StringType),
    StructField("ClientID", IntegerType),
    StructField("DateOpen", DateType)
  ))
  val clientSchema = StructType(Array(
    StructField("ClientID", IntegerType),
    StructField("ClientName", StringType),
    StructField("Type", StringType),
    StructField("Form", StringType),
    StructField("RegisterDate", DateType)
  ))
  val operationSchema = StructType(Array(
    StructField("AccountDB", IntegerType),
    StructField("AccountCR", IntegerType),
    StructField("DateOp", DateType),
    StructField("Amount", FloatType),
    StructField("Currency", StringType),
    StructField("Comment", StringType)
  ))
  val rateSchema = StructType(Array(
    StructField("Currency", StringType),
    StructField("Rate", StringType),
    StructField("RateDate", DateType)
  ))

  val accountDf = spark.read
    .format("csv")
    .option("header", "true")
    .option("delimiter","|")
    .schema(accountSchema)
    .load("src/main/scala/Account.csv")
  val clientDf = spark.read
    .format("csv")
    .option("header", "true")
    .option("delimiter","|")
    .schema(clientSchema)
    .load("src/main/scala/Clients.csv")
  val operationDf = spark.read
    .format("csv")
    .option("header", "true")
    .option("delimiter","|")
    .schema(operationSchema)
    .load("src/main/scala/Operation.csv")
    .withColumn("Amount",regexp_replace(col("Amount"), ",","."))
    .withColumn("Amount",col("Amount").cast(DoubleType))
  val rateDf = spark.read
    .format("csv")
    .option("header", "true")
    .option("delimiter","|")
    .schema(rateSchema)
    .load("src/main/scala/rate.csv")
    .withColumn("Rate",regexp_replace(col("Rate"), ",","."))
    .withColumn("Rate",col("Rate").cast(DoubleType))


  val acc_cl = accountDf
    .join(clientDf, accountDf("ClientID") === clientDf("ClientID"))
    .drop(clientDf("ClientID"))
    .persist(StorageLevel.MEMORY_ONLY_SER)

  val optrate = rateDf.select(
    col("RateDate").as("DateRate"),
    col("Currency").as("CurRate"),
    col("Rate"))
    .where(!col("CurRate").like("RUB"))


  var daterates = operationDf.select(col("DateOp").as("Date")).distinct()
  daterates = daterates.crossJoin(optrate.select(col("CurRate").as("Currency")).distinct()
  )

  daterates = daterates
    .join(optrate, daterates("Date") ===optrate("DateRate")
      && daterates("Currency") ===optrate("CurRate"), "left")
    .drop("DateRate")
    .drop("CurRate")

  val show = Window.partitionBy("Currency").orderBy(desc("Rate"), asc("Date"))
  daterates = daterates.withColumn("Rate",last("Rate", ignoreNulls = true) over show)

  val result = operationDf
    .join(acc_cl.select(
      col("AccountID").alias("AccDB"),
      col("AccountNum").alias("NumDB"),
      col("ClientID").alias("ClientDB"),
      col("Type").alias("TypeDB")
    ), col("AccDB") === col("AccountDB") || col("AccDB") === col("AccountCR"), "left")
    .join(acc_cl.select(
      col("AccountID").alias("AccCR"),
      col("AccountNum").alias("NumCR"),
      col("ClientID").alias("ClientCR"),
      col("Type").alias("TypeCR")
    ),
      col("AccCR") === col("AccountCR"), "left")
    .join(daterates, col("DateOp") === col("Date") && operationDf("Currency") === daterates("Currency"), "left")
    .drop(daterates("Currency"))
    .drop(daterates("Date"))

  val listAuto = raw"%а/м%, %а\м%, %автомобиль %, %автомобили %, %транспорт%, %трансп%средс%, %легков%, %тягач%, %вин%, %vin%,%viн:%, %fоrd%, %форд%,%кiа%, %кия%, %киа%%мiтsuвisнi%, %мицубиси%, %нissан%, %ниссан%, %sсанiа%, %вмw%, %бмв%, %аudi%, %ауди%, %jеер%, %джип%, %vоlvо%, %вольво%, %тоyота%, %тойота%, %тоиота%, %нyuнdаi%, %хендай%, %rенаulт%, %рено%, %реugеот%, %пежо%, %lаdа%, %лада%, %dатsuн%, %додж%, %меrсеdеs%, %мерседес%, %vоlкswаgен%, %фольксваген%, %sкоdа%, %шкода%, %самосвал%, %rover%, %ровер%"
  val listEat = raw"% сою%, %соя%, %зерно%, %кукуруз%, %масло%, %молок%, %молоч%, %мясн%, %мясо%, %овощ%, %подсолн%, %пшениц%, %рис%, %с/х%прод%, %с/х%товар%, %с\х%прод%, %с\х%товар%, %сахар%, %сельск%прод%, %сельск%товар%, %сельхоз%прод%, %сельхоз%товар%, %семен%, %семечк%, %сено%, %соев%, %фрукт%, %яиц%, %ячмен%, %картоф%, %томат%, %говя%, %свин%, %курин%, %куриц%, %рыб%, %алко%, %чаи%, %кофе%, %чипс%, %напит%, %бакале%, %конфет%, %колбас%, %морож%, %с/м%, %с\м%, %консерв%, %пищев%, %питан%, %сыр%, %макарон%, %лосос%, %треск%, %саир%, % филе%, % хек%, %хлеб%, %какао%, %кондитер%, %пиво%, %ликер%"

  val condAuto = listAuto.map(condition => !col("Comment").like(raw"$condition")).reduce(_ || _)
  val condEat = listEat.map(condition => !col("Comment").like(raw"$condition")).reduce(_ || _)

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
      round(sum(when(col("AccDB") ===col("AccountDB"), convertRate))).as("PaymentAmt"),
      round(sum(when(col("AccDB") ===col("AccountCR"), convertRate))).as("EnrollmentAmt"),
      round(sum(when(col("AccDB") ===col("AccountDB") && col("NumCR").startsWith("40702"), convertRate))).as("TaxAmt"),
      round(sum(when(col("AccDB") ===col("AccountCR") && col("NumDB").startsWith("40802"), convertRate))).as("ClearAmt"),
      round(sum(when(col("AccDB") ===col("AccountDB") && condAuto, convertRate))).as("AutoAmt"),
      round(sum(when(col("AccDB") ===col("AccountCR") && condEat, convertRate))).as("EatAmt"),
      round(sum(when(col("AccDB") ===col("AccountDB") && col("TypeCR") === "0", convertRate))).as("FLAmt")
    ).orderBy("AccDB", "DateOp").show()

  Thread.sleep(Int.MaxValue)
  // 2 витрина
  /*val corporate_account = corporate_payments
    .join(clientAndAccount.select($"AccountNum", $"AccountID", $"DateOpen", $"ClientName"), "AccountID")
    .withColumn("TotalAmt", round($"PaymentAmt"+$"EnrollementAmt",2))
    .select($"AccountID", $"AccountNum", $"DateOpen", $"ClientId", $"ClientName", $"TotalAmt", $"CutoffDt")
    .orderBy("AccountID", "CutoffDt")*/
  // 3 витрина
  /*val corporate_info = clientAndAccount
    .join(corporate_account.select($"TotalAmt", $"CutoffDt", $"AccountID"),
      corporate_account("AccountID") === clientAndAccount("AccountID"), "left")
    .withColumn("TotalAmt", sum("TotalAmt").over(Window.partitionBy("ClientId", "CutoffDt")))
    .select("ClientId", "ClientName", "Type", "Form", "RegisterDate", "TotalAmt", "CutoffDt")
    .distinct()
    .orderBy("ClientId")*/
}

