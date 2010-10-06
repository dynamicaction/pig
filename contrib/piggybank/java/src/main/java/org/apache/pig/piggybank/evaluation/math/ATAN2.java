/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.piggybank.evaluation.math;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.AtomSchema;
import org.apache.pig.impl.logicalLayer.schema.Schema;
/**
 * math.ATAN2 implements a binding to the Java function
* {@link java.lang.Math#atan2(double,double) Math.atan2(double,double)}. Given a tuple with
* two data atom x and y it will returns the angle theta from the conversion of 
* rectangular coordinates (x, y) to polar coordinates (r, theta).
* 
* <dl>
* <dt><b>Parameters:</b></dt>
* <dd><code>value</code> - <code>Tuple containing two DataAtom [double]</code>.</dd>
* 
* <dt><b>Return Value:</b></dt>
* <dd><code>DataAtom [double]</code> </dd>
* 
* <dt><b>Return Schema:</b></dt>
* <dd>atan2_inputSchema</dd>
* 
* <dt><b>Example:</b></dt>
* <dd><code>
* register math.jar;<br/>
* A = load 'mydata' using PigStorage() as ( float1 );<br/>
* B = foreach A generate float1, math.ATAN2(float1);
* </code></dd>
* </dl>
* 
* @see Math#atan2(double,double)
* @see
* @author ajay garg
*
*/
public class ATAN2 extends EvalFunc<DataAtom>{
	/**
	 * java level API
	 * @param input expects a tuple containing two numeric DataAtom value
	 * @param output returns a single numeric DataAtom value
	 */
	@Override
	public void exec(Tuple input, DataAtom output) throws IOException {
		output.setValue(atan2(input));
	}
	
	protected double atan2(Tuple input) throws IOException{
		try{
			double first = input.getAtomField(0).numval();
			double second = input.getAtomField(1).numval();
			return Math.atan2(first, second);
		}
		catch(RuntimeException e){
			throw new IOException("invalid input " + e.getMessage());
		}
		
	}
	
	@Override
	public Schema outputSchema(Schema input) {
		return new AtomSchema("atan2_"+input.toString()); 
	}

}
