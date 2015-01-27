/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package net.spright.id;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 * @author Tsai ChiaPing <chia7712@gmail.com>
 */
public class UUIDServer implements Runnable, Closeable {
    private final ServerSocket server;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    public UUIDServer(int port) throws IOException {
        server = new ServerSocket(port);
    }

    @Override
    public void run() {
        int currentValue = 0;
        while(!isClosed.get()) {
            try(Socket socket = server.accept()) {
                DataOutputStream output = new DataOutputStream(socket.getOutputStream());
                output.writeInt(currentValue);
                ++currentValue;
            } catch (IOException ex){}
        }
    }

    @Override
    public void close() throws IOException {
        isClosed.set(true);
        server.close();
    }
}
