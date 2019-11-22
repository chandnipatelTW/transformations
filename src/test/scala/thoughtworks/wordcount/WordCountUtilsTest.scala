package thoughtworks.wordcount

import org.apache.spark.sql.Row
import thoughtworks.DefaultFeatureSpecWithSpark


class WordCountUtilsTest extends DefaultFeatureSpecWithSpark {
  feature("Split Words") {
    scenario("test splitting a dataset of words by spaces") {
      Given("words with multiple spaces")
      import spark.implicits._
      val inputDS = Seq("one space", "many   spaces", "  at start", "at end ").toDS()

      val outputDS = WordCountUtils.StringDataset(inputDS).splitWords(spark)

      Then("should result dataset of all words without spaces")
      val  expectedDS = Seq("one", "space", "many", "spaces", "at", "start", "at", "end").toDS()
      assert(expectedDS.collect().toSeq == outputDS.collect().toSeq)
    }

    scenario("test splitting a dataset of words by period") {
      Given("words with multiple period")
      import spark.implicits._
      val inputDS = Seq(
        ".startPeriod", "middle.period", "endPeriod.",
        "many.periods...he.re").toDS()

      When("split words is called")
      val outputDS = WordCountUtils.StringDataset(inputDS).splitWords(spark)

      Then("should result dataset of all words without period")
      val  expectedDS = Seq(
        "startPeriod", "middle", "period", "endPeriod",
        "many", "periods", "he", "re"
      ).toDS()
      assert(expectedDS.collect().toSeq == outputDS.collect().toSeq)
    }

    scenario("test splitting a dataset of words by comma") {
      Given("words with multiple comma")
      import spark.implicits._
      val inputDS = Seq(
        ",start",
        "middle,comma",
        "end,",
      ",many,comma,,,in,string,").toDS()

      When("split words is called")
      val outputDS = WordCountUtils.StringDataset(inputDS).splitWords(spark)

      Then("should result dataset of all words without comma")
      val  expectedDS = Seq(
        "start", "middle", "comma", "end",
        "many", "comma", "in", "string"
      ).toDS()

      assert(expectedDS.collect().toSeq == outputDS.collect().toSeq)
    }

    scenario("test splitting a dataset of words by hyphen") {
      Given("words with multiple hyphen")
      import spark.implicits._
      val inputDS = Seq(
        "-start",
        "middle-hyphen",
        "end-",
        "-many-hyphen---in-string-").toDS()

      When("split words is called")
      val outputDS = WordCountUtils.StringDataset(inputDS).splitWords(spark)

      Then("should result dataset of all words without hyphen")
      val  expectedDS = Seq(
        "start", "middle", "hyphen", "end",
        "many", "hyphen", "in", "string"
      ).toDS()

      assert(expectedDS.collect().toSeq == outputDS.collect().toSeq)
    }

    scenario("test splitting a dataset of words by semi-colon") {
      Given("words with multiple semi-colon")
      import spark.implicits._
      val inputDS = Seq(
        ";start",
        "middle;semicolon",
        "end;",
        ";many;semicolon;;;in;string;").toDS()

      When("split words is called")
      val outputDS = WordCountUtils.StringDataset(inputDS).splitWords(spark)

      Then("should result dataset of all words without semicolon")
      val  expectedDS = Seq(
        "start", "middle", "semicolon", "end",
        "many", "semicolon", "in", "string"
      ).toDS()

      assert(expectedDS.collect().toSeq == outputDS.collect().toSeq)
    }

    scenario("test splitting a dataset of words by double quotes") {
      Given("words with multiple double quotes")
      import spark.implicits._
      val inputDS = Seq(
        "\"start",
        "middle\"semicolon",
        "end\"",
        "\"many\"semicolon\"\"in\"string\"").toDS()

      When("split words is called")
      val outputDS = WordCountUtils.StringDataset(inputDS).splitWords(spark)

      Then("should result dataset of all words without double quotes")
      val  expectedDS = Seq(
        "start", "middle", "semicolon", "end",
        "many", "semicolon", "in", "string"
      ).toDS()

      assert(expectedDS.collect().toSeq == outputDS.collect().toSeq)
    }

    scenario("test splitting a dataset of words with separators at start or end") {
      Given("words with separator at start")
      import spark.implicits._
      val inputDS = Seq("  spaces", ",comma", "--hyphen", "end , ").toDS()

      val outputDS = WordCountUtils.StringDataset(inputDS).splitWords(spark)

      Then("should result dataset without empty words and separators")
      val  expectedDS = Seq("spaces", "comma", "hyphen", "end").toDS()
      assert(expectedDS.collect().toSeq == outputDS.collect().toSeq)
    }

    scenario("test splitting a dataset of words with all separators") {
      Given("words with mutiple separators")
      import spark.implicits._
      val inputDS = Seq(" space.period", ",comma,hyphen-", "semi-colon;dot",
        ",all.the-separators; here", "?Hello!World(some)*45").toDS()

      val outputDS = WordCountUtils.StringDataset(inputDS).splitWords(spark)

      Then("should result dataset without empty words and separators")
      val  expectedDS = Seq("space", "period", "comma", "hyphen",
      "semi", "colon", "dot", "all", "the", "separators", "here",
        "Hello", "World", "some", "45").toDS()
      assert(expectedDS.collect().toSeq == outputDS.collect().toSeq)
    }
  }

  feature("Count Words") {
    scenario("test count for all distinct words") {
      Given("dataset with distinct words")
      import spark.implicits._
      val inputDS = Seq("one", "two").toDS()

      When("countByWords is called")
      val outputDS = WordCountUtils.StringDataset(inputDS).countByWord(spark)

      Then("should result in dataframe with word counts")
      val expectedDS = Seq(Row("one", 1), Row("two", 1))
      outputDS.collect().toSeq should be(expectedDS)
    }

    scenario("test count for similar words") {
      Given("dataset with similar words")
      import spark.implicits._
      val inputDS = Seq("one", "two", "two").toDS()

      When("countByWords is called")
      val outputDS = WordCountUtils.StringDataset(inputDS).countByWord(spark)

      Then("should result in dataset with word counts")
      val expectedDS = Seq(Row("one", 1), Row("two", 2))
      outputDS.collect().toSeq should contain theSameElementsAs expectedDS
    }

    scenario("test case insensitivity in words") {
      Given("dataset with different case words")
      import spark.implicits._
      val inputDS = Seq("one", "two", "tWo", "Two").toDS()

      When("countByWords is called")
      val outputDS = WordCountUtils.StringDataset(inputDS).countByWord(spark)

      Then("should result in dataset with word counts irrespective of case")
      val expectedDS = Seq(Row("one", 1), Row("two", 3))
      outputDS.collect().toSeq should contain theSameElementsAs expectedDS
    }
  }

  feature("Sort Words") {
    scenario("test ordering words") {
      Given("dataset of WordCount")
      import spark.implicits._
      val wordCountDS = Seq("one", "two", "two").toDS()
      When("sort is called")
      val sortedDS = WordCountUtils.StringDataset(wordCountDS).countByWord(spark)

      Then("should result in dataset ordered by word")
      val expectedDS = Seq(Row("one", 1), Row("two", 2))
      sortedDS.collect().toSeq should be (expectedDS)
    }

    scenario("test ordering words with apostrophe") {
      Given("dataset of WordCount")
      import spark.implicits._
      val wordCountDS = Seq("you've", "you").toDS()

      When("sort is called")
      val sortedDS = WordCountUtils.StringDataset(wordCountDS).countByWord(spark)

      Then("should result in dataset ordered by word")
      val expectedDS = Seq(Row("you", 1), Row("you've", 1))
      sortedDS.collect().toSeq should be (expectedDS)
    }
  }

}
