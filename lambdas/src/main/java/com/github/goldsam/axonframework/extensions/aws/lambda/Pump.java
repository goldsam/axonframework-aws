package com.github.goldsam.axonframework.extensions.aws.lambda;

import com.amazonaws.services.lambda.runtime.Context; 
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class Pump implements RequestHandler<Integer, String>{
    public String myHandler(int myCount, Context context) {
        return String.valueOf(myCount);
    }

    @Override
    public String handleRequest(Integer input, Context context) {
        return null;
    }
}
