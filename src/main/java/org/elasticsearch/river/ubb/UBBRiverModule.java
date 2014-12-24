package org.elasticsearch.river.ubb;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

/**
 * 
 * @author Hemed Ali, UB
 * @Date : 17-12-2014
 * Abstract: Bind the River class to UBBRiver class as EagerSingleton pattern. 
 * This means the instance of a class will be created on the system start-up (when elasticsearch starts).
 */
public class UBBRiverModule extends AbstractModule {
    
		@Override
		public void configure(){
			bind(River.class).to(UBBRiver.class).asEagerSingleton();
		}
}
