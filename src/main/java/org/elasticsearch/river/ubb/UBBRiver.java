package org.elasticsearch.river.ubb;

import org.elasticsearch.client.Client;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import org.elasticsearch.ElasticsearchIllegalArgumentException;

import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.ubb.support.Harvester;
import org.elasticsearch.river.ubb.settings.UBBRiverSettings;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import java.util.Map;

/**
 *
 * @author Hemed Ali
 * @Date : 17-12-2014
 * Abstract: This class get settings provided and initializes the river. 
 *
 */
public class UBBRiver extends AbstractRiverComponent implements River {
    
	private volatile Harvester harvester;
	private volatile Thread harvesterThread;
        
        /**
         * Using Google Guice to inject (pass as patameters) dependencies upon the creation of this instance (at run-time). 
         * Inject acts like an abstract class that manages dependencies for you.
         * This approach makes the application loosely coupled and adheres to the dependency inversion 
         * and single responsibility principles.  See Google Guice for more details.
         **/

	@Inject
	public UBBRiver(RiverName riverName, RiverSettings settings, @RiverIndexName String riverIndexName, Client client) 
        {
		super(riverName, settings);

		harvester = new Harvester().client(client);

		if (settings.settings().containsKey(UBBRiverSettings.UBB_RIVER)) 
                { 
	           Map<String, Object> ubbSettings = (Map<String, Object>)settings.settings().get(UBBRiverSettings.UBB_RIVER);
                  //TO DO: Many settings to be configured here. 
                   harvester.log("--------------------------------------------");
                   harvester.log("These are the supplied settings: " + settings.settings().toString());
		}
		else 
                {  
                   harvester.log("Settings are incomplete.");
	           throw new ElasticsearchIllegalArgumentException("No 'ubb' key was found. Please specify a key in the settings");
		}
                
                //if index key was found in the settings, then put it's value in use.
		if(settings.settings().containsKey("index"))
                {
			Map<String, Object> indexSettings = (Map<String, Object>)settings.settings().get("index");
			
                        harvester.setIndexName(XContentMapValues.nodeStringValue(indexSettings.get("index"), UBBRiverSettings.DEFAULT_INDEX_NAME))
				 .setIndexType(XContentMapValues.nodeStringValue(indexSettings.get("type"), UBBRiverSettings.DEFAULT_TYPE_NAME));
		}
		else //Use dafult settings if 'index' key was not found in the settings.
                {
			harvester.setIndexName(UBBRiverSettings.DEFAULT_INDEX_NAME)
			         .setIndexType(UBBRiverSettings.DEFAULT_TYPE_NAME);
		}
	}

	@Override
	public void start() {
                logger.info("Starting UBB river:  {}",  riverName.name());
		harvesterThread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "eea_rdf_river(" + riverName().name() +")").newThread(harvester);
		harvesterThread.start();
	}

	@Override
	public void close() {
		logger.info("Closing UBB-ElasticSearch-River: {}",  riverName.name());
                if(harvesterThread != null)
                {
		      harvester.setClose(true);
		      harvesterThread.interrupt();
                }
	}
}
