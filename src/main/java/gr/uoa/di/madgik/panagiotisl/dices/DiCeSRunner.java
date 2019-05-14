package gr.uoa.di.madgik.panagiotisl.dices;

import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;

public class DiCeSRunner {

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		
		new DiCeS().execute(args);
	}

}
