// Импорт необходимых библиотек
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

// Создание SparkSession
val spark = SparkSession.builder()
  .appName("Spark Scala CSV Example")
  .master("local[*]")
  .getOrCreate()

// Определение данных
val data = Seq(
  ("1984", "George Orwell", "Science Fiction", 5000, 1949),
  ("The Lord of the Rings", "J.R.R. Tolkien", "Fantasy", 3000, 1954),
  ("To Kill a Mockingbird", "Harper Lee", "Southern Gothic", 4000, 1960),
  ("The Catcher in the Rye", "J.D. Salinger", "Novel", 2000, 1951),
  ("The Great Gatsby", "F. Scott Fitzgerald", "Novel", 4500, 1925)
)

// Создание DataFrame
val columns = Seq("title", "author", "genre", "sales", "year")
val df = spark.createDataFrame(data).toDF(columns: _*)

// Определение пути к файлу
val csvFilePath = "/content/books.csv"

// Функция для удаления файла, если он существует
import java.io.File
def deleteIfExists(path: String): Unit = {
  val file = new File(path)
  if (file.exists()) {
    file.delete()
  }
}

// Проверка существования файла и удаление, если существует
deleteIfExists(csvFilePath)

// Сохранение DataFrame в CSV-файл
df.write.option("header", "true").csv(csvFilePath)

// Чтение данных из CSV-файла
val booksDF = spark.read.option("header", "true").option("inferSchema", "true").csv(csvFilePath)

// Фильтрация данных, чтобы оставить только книги с продажами выше 3000 экземпляров
val filteredDF = booksDF.filter(col("sales") > 3000)

// Группировка данных по жанру и вычисление общего объема продаж для каждого жанра
val groupedDF = booksDF.groupBy("genre").agg(sum("sales").alias("total_sales"))

// Сортировка данных по общему объему продаж в порядке убывания
val sortedDF = groupedDF.orderBy(desc("total_sales"))

// Вывод результатов на экран с подписями
println("Original DataFrame:")
booksDF.show()

println("Filtered DataFrame (sales > 3000):")
filteredDF.show()

println("Grouped DataFrame by genre with total sales:")
groupedDF.show()

println("Sorted DataFrame by total sales:")
sortedDF.show()

// Остановка SparkSession
spark.stop()
