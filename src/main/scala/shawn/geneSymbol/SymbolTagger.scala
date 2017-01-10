package shawn.geneSymbol

import java.util.regex.Pattern
import java.io.File
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.util.Version
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.store.SimpleFSDirectory
import java.util.concurrent.atomic.AtomicLong
import scala.io.Source
import java.io.StringReader
import scala.collection.mutable.ArrayBuffer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import scala.text.Document
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field.Store
import org.apache.lucene.document.Field.Index
import java.util.stream.StreamSpliterators.ArrayBuffer
import org.apache.lucene.search.IndexSearcher

class SymbolTagger {
  //val tabPattern = "\\t";
  val punctPattern = Pattern.compile("\\p{Punct}")
  val spacePattern = Pattern.compile("\\s+")

  def buildIndex(inputFile: File,
    luceneDir: File): Unit = {
    // set up the index writer
    val analyzer = getAnalyzer()
    val iwconf = new IndexWriterConfig(Version.LUCENE_46, analyzer)
    iwconf.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
    val writer = new IndexWriter(new SimpleFSDirectory(luceneDir), iwconf)
    // read through input file and write out to lucene
    val counter = new AtomicLong(0L)
    val linesReadCounter = new AtomicLong(0L)
    Source.fromFile(inputFile)
      .getLines()
      .foreach(line => {
        val linesRead = linesReadCounter.incrementAndGet()
        if (linesRead % 1000 == 0) Console.println("%d lines read".format(linesRead))
        val Array(cui, str) = line.split("\t")
        val strNorm = normalizeCasePunct(str)
        val strSorted = sortWords(strNorm)
        val strStemmed = stemWords(strNorm)
        // write full str record
        // str = exact string
        // str_norm = case and punct normalized, exact
        // str_sorted = str_norm sorted
        // str_stemmed = str_sorted stemmed
        val fdoc = new Document()
        val fid = counter.incrementAndGet()
        fdoc.add(new Field("id", fid.toString, Store.YES, Index.NOT_ANALYZED))
        fdoc.add(new Field("cui", cui, Store.YES, Index.NOT_ANALYZED))
        fdoc.add(new Field("str", str, Store.YES, Index.NOT_ANALYZED))
        fdoc.add(new Field("str_norm", strNorm, Store.YES, Index.NOT_ANALYZED))
        fdoc.add(new Field("str_sorted", strSorted, Store.YES, Index.NOT_ANALYZED))
        fdoc.add(new Field("str_stemmed", strStemmed, Store.YES, Index.NOT_ANALYZED))
        writer.addDocument(fdoc)
        if (fid % 1000 == 0) writer.commit()
      })
    writer.commit()
    writer.close()
  }

  def annotateConcepts(phrase: String,
    luceneDir: File): List[(Double, String, String)] = {
    val suggestions = ArrayBuffer[(Double, String, String)]()
    val reader = DirectoryReader.open(
      new SimpleFSDirectory(luceneDir))
    val searcher = new IndexSearcher(reader)
    // try to match full string
    suggestions ++= cascadeSearch(searcher, reader, 
        phrase, 1.0)
    if (suggestions.size == 0) {
      // no exact match found, fall back to inexact matches
      val words = normalizeCasePunct(phrase)
        .split(" ")
    }
  }

  def stemWords(str: String): String = {
    val stemmedWords = ArrayBuffer[String]()
    val tokenStream = getAnalyzer().tokenStream(
      "str_stemmed", new StringReader(str))
    val ctattr = tokenStream.addAttribute(
      classOf[CharTermAttribute])
    tokenStream.reset()
    while (tokenStream.incrementToken()) {
      stemmedWords += ctattr.toString()
    }
    stemmedWords.mkString(" ")
  }

  def sortWords(str: String): String = {
    val words = str.split(" ")
    words.sortWith(_ < _).mkString(" ")
  }

  def normalizeCasePunct(str: String): String = {
    val str_lps = punctPattern
      .matcher(str.toLowerCase())
      .replaceAll(" ")

    spacePattern.matcher(str_lps).replaceAll(" ")

  }

  def getAnalyzer(): Analyzer = {
    new StandardAnalyzer(Version.LUCENE_46)
  }
}