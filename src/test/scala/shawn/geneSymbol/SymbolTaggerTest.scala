package shawn.geneSymbol

import org.junit.Test
import org.junit.Before

class SymbolTaggerTest {
  
  var symbolTagger: SymbolTagger = _

  @Before
  def setup() {
    symbolTagger = new SymbolTagger()
  }

  @Test
  def test() {
    val str = "there is a cat in the garden!"
    println(symbolTagger.stemWords(str))
  }
}