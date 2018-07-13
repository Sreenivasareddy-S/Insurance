CREATE TABLE insurance(id INTEGER,
                            type VARCHAR(256) NOT NULL UNIQUE,
                            PRIMARY KEY (id)
                            );

CREATE TABLE insuranceAttributes(id INTEGER,
                            insuranceID INTEGER REFERENCES insuranceID(id),
                            name VARCHAR(256),   /* Name of the attribute/field */
                            dataType INTEGER, /* type of the data  for this attribute */ 
                            PRIMARY KEY (id)
                            );
 
CREATE TABLE insured(id INTEGER,
                            insuranceID INTEGER REFERENCES insurance(id), /*Insurance type to which this attribute belongs to*/
                            PRIMARY KEY (id)
                            ); 

CREATE TABLE insuredData(id INTEGER, insuredID INTEGER REFERENCES insured(id), /* Insured ID to whome/which this data belongs to*/
			name VARCHAR(256), /*Attribuete/Field name */
                        opaqueValue VARCHAR(4096)); /*Attribute value, in opaque format */  	
