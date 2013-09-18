package com.cdyne.ws;

import java.rmi.RemoteException;

import com.cdyne.ws.CheckStub.CheckTextBodyV2;
import com.cdyne.ws.CheckStub.Words;

public class SpellCorrect {
	public String correctSentence(String sentence) throws RemoteException
	{
		CheckStub stub=new CheckStub();
		CheckTextBodyV2 textBody=new CheckTextBodyV2();
		textBody.setBodyText(sentence);
		Words[] replaceWord=stub.checkTextBodyV2(textBody).getDocumentSummary().getMisspelledWord();
		for(int i=0;i<replaceWord.length;i++)
		{
			String[] suggestions=replaceWord[i].getSuggestions();
			String wordToBeReplaced=replaceWord[i].getWord();
			sentence=sentence.replaceAll(wordToBeReplaced,suggestions[0].toString());
		}
		return sentence;
	}

}
