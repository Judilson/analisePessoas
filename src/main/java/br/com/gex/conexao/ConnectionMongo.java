/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.gex.conexao;

import com.mongodb.MongoClient;

/**
 *
 * @author jjunior
 */
public class ConnectionMongo {

    
    public static final String ENDERECOESCRITA = "localhost";
    
    //public static final String ENDERECOESCRITA = "173.22.21.22";
    //public static final String ENDERECOLEITURA1 = "173.22.21.24";
    //public static final String ENDERECOLEITURA2 = "173.22.21.28";

    public static final int PORTAESCRITA = 27017;
    //public static final int PORTALEITURA1 = 27018;
    //public static final int PORTALEITURA2 = 27019;

    private static ConnectionMongo uniqInstance;

    private MongoClient mongoClient;

    private ConnectionMongo() {
    }

    public static synchronized ConnectionMongo getInstance() {
        if (uniqInstance == null) {
            uniqInstance = new ConnectionMongo();
        }
        return uniqInstance;
    }

    public MongoClient getClient(String endereco, int porta, String usuario, String senha) {
        if (mongoClient == null) {
            mongoClient = new MongoClient(endereco, porta);
        }
        return mongoClient;
    }

//    public static synchronized MongoClient conexao(String endereco, int porta, String usuario, String senha) {
//
//        
//        
//        mongoClient = new MongoClient(endereco, porta);
//
//        return mongoClient;
//    }
}
