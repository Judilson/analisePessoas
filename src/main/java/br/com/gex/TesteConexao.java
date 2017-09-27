/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.gex;

import br.com.gex.conexao.ConnectionMongo;
import br.com.gex.conexao.ConnectionOracle;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 *
 * @author jjunior
 */
public class TesteConexao {

    public static void main(String[] argv) {

        DB db = ConnectionMongo.getInstance().getClient(ConnectionMongo.ENDERECOESCRITA, ConnectionMongo.PORTAESCRITA, null, null).getDB("giex");

        DBCollection collection = db.getCollection("giex");

        try {

            Connection conn = new ConnectionOracle().conecta();
            PreparedStatement psmtPessoa = null;
            PreparedStatement psmtCadastro = null;
            PreparedStatement psmtLancamentos = null;

            String queryPessoa = "select pess_id_pessoa, cred_id_credor from giexbase.tb_pessoas_indicadores"
                    + " where pein_st_lote = 'S' and rownum < 100";

            String queryCadastro = "select cada_id_cadastro from giexbase.tb_cadastros_pessoas "
                    + " where pess_id_pessoa = ?id_pessoa";

            String queryLancamentos = "SELECT   li.lanc_id_lancamento,"
                    + "         laid_st_suspenso,"
                    + "         laid_st_prescrito,"
                    + "         laid_st_aberto,"
                    + "         laid_st_cancelado,"
                    + "         laid_st_pago"
                    + "  FROM       giexbase.tb_lancamentos_indicadores li"
                    + "         INNER JOIN"
                    + "             giexbase.tb_lancamentos l"
                    + "         ON li.lanc_id_lancamento = l.lanc_id_lancamento"
                    + " WHERE   l.cred_id_credor = ?id_credor"
                    + "         AND l.cada_id_cadastro IN (SELECT   cada_id_cadastro"
                    + "                                      FROM   giexbase.tb_cadastros_pessoas"
                    + "                                     WHERE   pess_id_pessoa = ?id_pessoa)";
            psmtPessoa = conn.prepareStatement(queryPessoa);
            //psmt.setInt(1, Integer.parseInt(request.getParameter("idCredor")));

            ResultSet resultSetPess = psmtPessoa.executeQuery();

            while (resultSetPess.next()) {

                BasicDBObject documentPessoa = new BasicDBObject();
                BasicDBObject documentCadastro = new BasicDBObject();
                BasicDBObject documentLancamento = new BasicDBObject();
                BasicDBObject documentLancamento_suspeso = new BasicDBObject();
                BasicDBObject documentLancamento_aberto = new BasicDBObject();
                BasicDBObject documentLancamento_cancelado = new BasicDBObject();
                BasicDBObject documentLancamento_pago = new BasicDBObject();
                BasicDBObject documentLancamento_prescrito = new BasicDBObject();

                if (resultSetPess.getMetaData().getColumnClassName(1) == "java.math.BigDecimal") {
                    documentPessoa.put(resultSetPess.getMetaData().getColumnLabel(1), Double.parseDouble(resultSetPess.getObject(1).toString()));
                } else {
                    documentPessoa.put(resultSetPess.getMetaData().getColumnLabel(1), resultSetPess.getObject(1));
                }

                psmtCadastro = conn.prepareStatement(queryCadastro);
                psmtCadastro.setObject(1, resultSetPess.getObject("pess_id_pessoa"));

                ResultSet resultSetCad = psmtCadastro.executeQuery();

                int totalRowsCadastro = resultSetCad.getMetaData().getColumnCount();

                for (int i = 1; i <= totalRowsCadastro; i++) {
                    try {
                        if (resultSetCad.getMetaData().getColumnClassName(i) == "java.math.BigDecimal") {
                            documentCadastro.put(resultSetCad.getMetaData().getColumnLabel(i), Double.parseDouble(resultSetCad.getObject(i).toString()));
                        } else {
                            documentCadastro.put(resultSetCad.getMetaData().getColumnLabel(i), resultSetCad.getObject(i));
                        }
                    } catch (Exception e) {
                    }
                }
                documentPessoa.put("Cadastros", documentCadastro);

                psmtLancamentos = conn.prepareStatement(queryLancamentos);
                psmtLancamentos.setObject(2, resultSetPess.getObject("pess_id_pessoa"));
                psmtLancamentos.setObject(2, resultSetPess.getObject("cred_id_credor"));

                ResultSet resultSetLanc = psmtLancamentos.executeQuery();

                int totalRowsLancamentos = resultSetCad.getMetaData().getColumnCount();

                for (int i = 1; i <= totalRowsLancamentos; i++) {

                    try {
                        if (resultSetLanc.getMetaData().getColumnLabel(i) == "LAID_ST_SUSPENSO"
                                && resultSetLanc.getObject(i) == "S") {
                            if (resultSetLanc.getMetaData().getColumnClassName(i) == "java.math.BigDecimal") {
                                documentLancamento_suspeso.put(resultSetLanc.getMetaData().getColumnLabel(i), Double.parseDouble(resultSetLanc.getObject(i).toString()));
                            } else {
                                documentLancamento_suspeso.put(resultSetLanc.getMetaData().getColumnLabel(i), resultSetLanc.getObject(i));
                            }
                        }
                    } catch (Exception e) {
                    }
                    
                    try {
                        if (resultSetLanc.getMetaData().getColumnLabel(i) == "LAID_ST_PRESCRITO"
                                && resultSetLanc.getObject(i) == "S") {
                            if (resultSetLanc.getMetaData().getColumnClassName(i) == "java.math.BigDecimal") {
                                documentLancamento_prescrito.put(resultSetLanc.getMetaData().getColumnLabel(i), Double.parseDouble(resultSetLanc.getObject(i).toString()));
                            } else {
                                documentLancamento_prescrito.put(resultSetLanc.getMetaData().getColumnLabel(i), resultSetLanc.getObject(i));
                            }
                        }
                    } catch (Exception e) {
                    }
                    
                    try {
                        if (resultSetLanc.getMetaData().getColumnLabel(i) == "LAID_ST_ABERTO"
                                && resultSetLanc.getObject(i) == "S") {
                            if (resultSetLanc.getMetaData().getColumnClassName(i) == "java.math.BigDecimal") {
                                documentLancamento_aberto.put(resultSetLanc.getMetaData().getColumnLabel(i), Double.parseDouble(resultSetLanc.getObject(i).toString()));
                            } else {
                                documentLancamento_aberto.put(resultSetLanc.getMetaData().getColumnLabel(i), resultSetLanc.getObject(i));
                            }
                        }
                    } catch (Exception e) {
                    }
                    
                    try {
                        if (resultSetLanc.getMetaData().getColumnLabel(i) == "LAID_ST_CANCELADO"
                                && resultSetLanc.getObject(i) == "S") {
                            if (resultSetLanc.getMetaData().getColumnClassName(i) == "java.math.BigDecimal") {
                                documentLancamento_cancelado.put(resultSetLanc.getMetaData().getColumnLabel(i), Double.parseDouble(resultSetLanc.getObject(i).toString()));
                            } else {
                                documentLancamento_cancelado.put(resultSetLanc.getMetaData().getColumnLabel(i), resultSetLanc.getObject(i));
                            }
                        }
                    } catch (Exception e) {
                    }
                    
                   try {
                        if (resultSetLanc.getMetaData().getColumnLabel(i) == "LAID_ST_PAGO"
                                && resultSetLanc.getObject(i) == "S") {
                            if (resultSetLanc.getMetaData().getColumnClassName(i) == "java.math.BigDecimal") {
                                documentLancamento_pago.put(resultSetLanc.getMetaData().getColumnLabel(i), Double.parseDouble(resultSetLanc.getObject(i).toString()));
                            } else {
                                documentLancamento_pago.put(resultSetLanc.getMetaData().getColumnLabel(i), resultSetLanc.getObject(i));
                            }
                        }
                    } catch (Exception e) {
                    }
                }
                documentPessoa.put("Cadastros", documentCadastro);

                documentLancamento.put("LancamentosAberto", documentLancamento_aberto);
                documentLancamento.put("LancamentosCancelado", documentLancamento_cancelado);
                documentLancamento.put("LancamentosPago", documentLancamento_pago);
                documentLancamento.put("LancamentosSuspenso", documentLancamento_suspeso);
                documentLancamento.put("LancamentosPrescrito", documentLancamento_prescrito);

                documentPessoa.put("Lancamentos", documentLancamento);

                collection.insert(documentPessoa);

            }

            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
