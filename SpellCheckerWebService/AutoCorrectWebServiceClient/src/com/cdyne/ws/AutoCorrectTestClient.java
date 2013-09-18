package com.cdyne.ws;

import java.io.File;
import java.io.FileWriter;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

import com.cdyne.ws.SpellCorrectStub.CorrectSentence;



public class AutoCorrectTestClient {
	public static void main(String args[]) throws RemoteException, SpellCorrectRemoteExceptionException 
	{
		//input string
		String testString="Helllo! Worlld";
		//C:/SatProject/AutoCorrectWebService/news.txt
		//C:/SatProject/AutoCorrectWebService/corrected.txt
		String newsFile="news.txt";
		String correctedFile="corrected.txt";
		File input=new File(newsFile);
		FileWriter fw=null;
		Scanner scan=null;
		String correctedSentence=null;
		try
		{
		scan = new Scanner(input);
		fw=new FileWriter(correctedFile);
		}
		catch(Exception e)
		{
			System.out.println("File not found, enter a valid file location and name");
		}
		String line="";
		while(scan.hasNextLine())
		{
			
			line+=scan.nextLine();
			line+="\r\n";
			
			
		}	
			SpellCorrectStub stubSpellCorrect=new SpellCorrectStub();
			CorrectSentence cs=new CorrectSentence();
			cs.setSentence(line);
			correctedSentence=stubSpellCorrect.correctSentence(cs).get_return();
			try
			{
				fw.append(correctedSentence);
				scan.close();	
				fw.close();
			}
			
			catch(Exception e)
			{
				System.out.println(e);
			}
			
	}
}
