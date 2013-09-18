
/**
 * SpellCorrectRemoteExceptionException.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis2 version: 1.6.0  Built on : May 17, 2011 (04:19:43 IST)
 */

package com.cdyne.ws;

public class SpellCorrectRemoteExceptionException extends java.lang.Exception{

    private static final long serialVersionUID = 1365444934043L;
    
    private com.cdyne.ws.SpellCorrectStub.SpellCorrectRemoteException faultMessage;

    
        public SpellCorrectRemoteExceptionException() {
            super("SpellCorrectRemoteExceptionException");
        }

        public SpellCorrectRemoteExceptionException(java.lang.String s) {
           super(s);
        }

        public SpellCorrectRemoteExceptionException(java.lang.String s, java.lang.Throwable ex) {
          super(s, ex);
        }

        public SpellCorrectRemoteExceptionException(java.lang.Throwable cause) {
            super(cause);
        }
    

    public void setFaultMessage(com.cdyne.ws.SpellCorrectStub.SpellCorrectRemoteException msg){
       faultMessage = msg;
    }
    
    public com.cdyne.ws.SpellCorrectStub.SpellCorrectRemoteException getFaultMessage(){
       return faultMessage;
    }
}
    