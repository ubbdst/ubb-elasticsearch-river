package org.elasticsearch.river.ubb.settings;
import java.util.regex.*;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author EEA, customized by Hemed Ali
 * Date : 16-12-2014 02:18 PM
 * Abstract: This file contains hard-corded settings for this River PLugin
 */
public abstract class UBBRiverSettings {

    
        public final static String UBB_RIVER = "ubb";
	public final static String DEFAULT_INDEX_NAME = "rdfdata";
	public final static String DEFAULT_TYPE_NAME = "resource";
        public final static String UBB_RIVER_NAME = "ubb-elasticsearch-river";
        public final static String UBB_RIVER_DESCRIPTION = "The University of Bergen Library River Plugin";
                
	public final static int DEFAULT_BULK_SIZE = 100;
	public final static int DEFAULT_BULK_REQ = 30;
	public final static List<String> DEFAULT_QUERIES = new ArrayList<String>();
	public final static String DEFAULT_ENDPOINT =	"http://semantic.eea.europa.eu/sparql";
	public final static String DEFAULT_QUERYTYPE = "construct";
	public final static String DEFAULT_PROPLIST = "[" +
		"\"http://purl.org/dc/terms/spatial\", " +
		"\"http://purl.org/dc/terms/creator\", " +
		"\"http://www.w3.org/1999/02/22-rdf-syntax-ns#type\", " +
		"\"http://purl.org/dc/terms/issued\", " +
		"\"http://purl.org/dc/terms/title\", " +
		"\"http://www.w3.org/1999/02/22-rdf-syntax-ns#about\", " +
		"\"language\", \"topic\"]";
	public final static String DEFAULT_LIST_TYPE = "white";
	public final static Boolean DEFAULT_ADD_LANGUAGE = true;
	public final static String DEFAULT_LANGUAGE = "\"en\"";
	public final static Boolean DEFAULT_ADD_URI = true;
	public final static String DEFAULT_URI_DESCRIPTION =
		"[http://www.w3.org/2000/01/rdf-schema#label,"
		+ "http://purl.org/dc/terms/title]";
	public final static String DEFAULT_SYNC_COND = "";
	public final static String DEFAULT_SYNC_TIME_PROP =
		"http://cr.eionet.europa.eu/ontologies/contreg.rdf#lastRefreshed";
	public final static Boolean DEFAULT_SYNC_OLD_DATA = false;
        
        public static String parseForJson(String text) {
        return text.trim().replaceAll("[\n\r]", " ")
                .replace('\"', '\'')
                .replace("\t", "    ")
                .replace("\\'", "\'")
                .replaceAll("\\\\x[a-fA-F0-9][a-fA-F0-9]", "_")
                .replace("\\", "\\\\");
       }

 }
