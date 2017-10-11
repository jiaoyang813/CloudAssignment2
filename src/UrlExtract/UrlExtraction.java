package UrlExtract;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.SAXException;

public class UrlExtraction {

	public static void main(String[] args) {

		//File fXmlFile = new File("/Users/oscar/Desktop/FirstArticle.tsv");
		ReadFile();
	}
	
	private static void ReadFile()
	{
		BufferedReader br = null;
		FileReader fr = null;

		BufferedWriter bw = null;
		FileWriter fw = null;
		
		ArrayList<String> articles = new ArrayList<String>();
		
		try {
			fr = new FileReader("/Users/oscar/Desktop/smallfile.tsv");
			br = new BufferedReader(fr);

			String sCurrentLine;

			while ((sCurrentLine = br.readLine()) != null) {
				articles.add(sCurrentLine);
			}
			
			for(String article : articles)
			{
			    ArrayList<String> urls = XmlParser(article);
			}
		} catch (IOException e) {

			e.printStackTrace();

		} finally {

			try {

				if (br != null)
					br.close();

				if (fr != null)
					fr.close();

			} catch (IOException ex) {

				ex.printStackTrace();

			}
		}
	}
	
	 private static ArrayList<String> XmlParser(String article)
	  {
	    String[] parts = article.split("\t");
	    String xmlfile = parts[3];
	    String currentUrl = "http://en.wikipedia.org/wiki/" + parts[1];
		ArrayList<String> urls = new ArrayList<String>();
		try {
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(new ByteArrayInputStream(xmlfile.getBytes()));
			doc.getDocumentElement().normalize();

//			System.out.println("Root element :" + doc.getDocumentElement().getNodeName());

			NodeList nodeList = doc.getElementsByTagName("param");
			
			for (int count = 0; count < nodeList.getLength(); count++) {

				Node tempNode = nodeList.item(count);

				// make sure it's element node.
				if (tempNode.getNodeType() == Node.ELEMENT_NODE) {

					// get node name and value
					if (tempNode.hasAttributes()) {
						// get attributes names and values
						NamedNodeMap nodeMap = tempNode.getAttributes();

						for (int i = 0; i < nodeMap.getLength(); i++) {
							Node node = nodeMap.item(i);
							if(node.getNodeValue().equals("url"))
							{
								String linkUrl = tempNode.getTextContent();
							    urls.add(currentUrl + "\t" + linkUrl);
							    System.out.println(currentUrl + "\t" + linkUrl);
							    break;
							}
						}

					}
				}
			}	
		}catch(Exception ex)
		{	
		}
		return urls;
	  }

}
