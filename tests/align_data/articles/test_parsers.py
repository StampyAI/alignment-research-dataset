from unittest.mock import Mock, patch

from align_data.sources.articles.parsers import medium_blog

SAMPLE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<TEI xml:space="preserve" xmlns="http://www.tei-c.org/ns/1.0"
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
xsi:schemaLocation="http://www.tei-c.org/ns/1.0 https://raw.githubusercontent.com/kermitt2/grobid/master/grobid-home/schemas/xsd/Grobid.xsd"
 xmlns:xlink="http://www.w3.org/1999/xlink">
    <teiHeader xml:lang="en">
        <fileDesc>
            <titleStmt>
                <title level="a" type="main">The title!!</title>
            </titleStmt>
            <sourceDesc>
                <biblStruct>
                    <analytic>
                        <author>
                            <persName><forename type="first">Cullen</forename><surname>OâKeefe</surname></persName>
                        </author>
                    </analytic>
                </biblStruct>
            </sourceDesc>
        </fileDesc>
        <encodingDesc>
            <appInfo>
                <application version="0.7.0" ident="GROBID" when="2022-03-25T06:04+0000">
                    <desc>GROBID - A machine learning software for extracting information from scholarly documents</desc>
                    <ref target="https://github.com/kermitt2/grobid"/>
                </application>
            </appInfo>
        </encodingDesc>
        <profileDesc>
            <abstract>this is the abstract</abstract>
       </profileDesc>
    </teiHeader>
    <text xml:lang="en">
        <body>This is the contents</body>
    </text>
</TEI>
"""

def test_medium_blog():
    html = """
    <article>
      <div>
        <h1>This is the title</h1>
        <div>
          <span>Some random thing</span>
          <span>another random thing</span>
          <span>Oct 7, 2023</span>
          <span>more random stuff</span>
         </div>
       </div>
       <div>
         bla bla bla <a href="http://ble.com">a link</a> bla bla
       </div>
    </article>
    """
    with patch('requests.get', return_value=Mock(content=html)):
        assert medium_blog('bla.com') == "bla bla bla [a link](http://ble.com) bla bla"


def test_medium_blog_no_title():
    html = """
    <article>
      <div>
          <span>Some random thing</span>
       </div>
       <div>
         bla bla bla <a href="http://ble.com">a link</a> bla bla
       </div>
    </article>
    """
    with patch('requests.get', return_value=Mock(content=html)):
        assert medium_blog('bla.com') == "Some random thing\n\n\n bla bla bla [a link](http://ble.com) bla bla"


def test_medium_blog_no_contents():
    html = """
    <div>
      <div>
          <span>Some random thing</span>
       </div>
       <div>
         bla bla bla <a href="http://ble.com">a link</a> bla bla
       </div>
    </div>
    """
    with patch('requests.get', return_value=Mock(content=html)):
        assert medium_blog('bla.com') is None
