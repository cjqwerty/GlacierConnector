package org.fcrepo.federation.glacierconnector;

import java.util.LinkedHashMap;

public class SizeFixedMap<K,E> extends LinkedHashMap<K,E>{
	private static final long serialVersionUID = 1L;
	private int maxsize = 10;
	
	SizeFixedMap(int maxsize){		
		this.maxsize = maxsize;		
	}

	@Override
	public E put(K key, E e){
		if(this.size() <= maxsize){
			return super.put(key,e);			
		}
		else{
			return null;
		}
	
	}
	

}
