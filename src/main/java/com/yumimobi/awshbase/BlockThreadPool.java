/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.hbasedemo;

/**
 *
 * @author yrd1989
 */
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 自定义阻塞型线程池 当池满时会阻塞任务提交
 * 
 * @ClassName: BlockThreadPool
 * @Description: TODO
 * @author: wangs
 * @date: 2018-1-24 下午5:24:54
 */
public class BlockThreadPool {

    private ThreadPoolExecutor pool = null;
    
    public BlockThreadPool(int poolSize) {
        pool = new ThreadPoolExecutor(poolSize, poolSize, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(5), new CustomThreadFactory(),
                new CustomRejectedExecutionHandler());
        
    }
    
    public void destory() {
        if (pool != null) {
            pool.shutdownNow();
        }
    }
    
    public Integer getQueueSize() {
        
        return pool.getQueue().size();
    }

    private class CustomThreadFactory implements ThreadFactory {
        private AtomicInteger count = new AtomicInteger(0);
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            String threadName = BlockThreadPool.class.getSimpleName() + count.addAndGet(1);
            t.setName(threadName);
            return t;
        }
    }

    private class CustomRejectedExecutionHandler implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            try {
                // 核心改造点，由blockingqueue的offer改成put阻塞方法
                executor.getQueue().put(r);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void execute(Runnable runnable) {
        this.pool.execute(runnable);
    }

    // 测试构造的线程池
    public static void main(String[] args) {
        BlockThreadPool pool = new BlockThreadPool(3);
        for (int i = 1; i < 20; i++) {
            System.out.println("getQueueSize:"+pool.getQueueSize());
            System.out.println("提交第" + i + "个任务!");
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        System.out.println(Thread.currentThread().getId() + "=====开始");
                        TimeUnit.SECONDS.sleep(10);
                        System.out.println(Thread.currentThread().getId() + "=====【结束】");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
            System.out.println("【提交第" + i + "个任务成功！】");
        }

        // 2.销毁----此处不能销毁,因为任务没有提交执行完,如果销毁线程池,任务也就无法执行了
        /*while (pool.getActiveCount() == 3 ) {
            pool.destory();
        }*/
        while (pool.getQueueSize()>0) {
            System.out.println("getQueue().size:"+pool.getQueueSize());
            try {
                Thread.sleep(10000);
            } catch (InterruptedException ex) {
                Logger.getLogger(BlockThreadPool.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
        pool.destory();
    }
}
