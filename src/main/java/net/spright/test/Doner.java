/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package net.spright.test;

import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author Tsai ChiaPing <chia7712@gmail.com>
 */
public interface Doner {
    public String toDo(String[] remainArgs, Configuration configuration) throws Exception;
}
