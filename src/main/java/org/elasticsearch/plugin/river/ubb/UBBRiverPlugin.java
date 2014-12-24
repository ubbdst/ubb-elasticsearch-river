package org.elasticsearch.plugin.river.ubb;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.river.ubb.UBBRiverModule;
import org.elasticsearch.river.ubb.settings.UBBRiverSettings;


/**
 * @author Hemed
 * Created : 16-12-2014 2:08 PM
 * Location : The University of Bergen Library
 * Abstract : This class extends AbstractPlugin class from ElasticSearch and override it's method.
 */

public class UBBRiverPlugin extends AbstractPlugin {

	@Inject //Create an empty constructor
	public UBBRiverPlugin(){}

	@Override  //This is the name that elasticsearch will use.
	public String name() {
		return UBBRiverSettings.UBB_RIVER_NAME;
	}

        /*This is the description for our River.
        */
	@Override
	public String description() {
		return UBBRiverSettings.UBB_RIVER_DESCRIPTION;
	}

        //Register our river module to ElasticSearch River Module
	public void onModule(RiversModule module) {
		module.registerRiver(UBBRiverSettings.UBB_RIVER, UBBRiverModule.class);
	}
}

