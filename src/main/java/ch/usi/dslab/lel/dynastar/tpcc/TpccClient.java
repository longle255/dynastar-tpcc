package ch.usi.dslab.lel.dynastar.tpcc;

import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.bezerra.netwrapper.codecs.Codec;
import ch.usi.dslab.bezerra.netwrapper.codecs.CodecUncompressedKryo;
import ch.usi.dslab.bezerra.sense.DataGatherer;
import ch.usi.dslab.bezerra.sense.monitors.ThroughputPassiveMonitor;
import ch.usi.dslab.lel.dynastar.tpcc.benchmark.BenchContext;
import ch.usi.dslab.lel.dynastar.tpcc.tpcc.TpccConfig;
import ch.usi.dslab.lel.dynastar.tpcc.tpcc.TpccRandom;
import ch.usi.dslab.lel.dynastar.tpcc.tpcc.TpccUtil;
import ch.usi.dslab.lel.dynastarv2.Client;
import ch.usi.dslab.lel.dynastarv2.Partition;
import ch.usi.dslab.lel.dynastarv2.command.Command;
import ch.usi.dslab.lel.dynastarv2.probject.ObjId;
import ch.usi.dslab.lel.dynastarv2.probject.PRObjectGraph;
import ch.usi.dslab.lel.dynastarv2.probject.PRObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.BinaryJedis;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static ch.usi.dslab.lel.dynastar.tpcc.tpcc.TpccConfig.*;

public class TpccClient {
    private static final Logger logger = LoggerFactory.getLogger(TpccClient.class);
    private Client clientProxy;
    private Random gen;
    private TpccTerminal[] terminals;
    private String[] terminalNames;
    private boolean terminalsBlockingExit = false;
    private long terminalsStarted = 0, sessionCount = 0, transactionCount = 0;
    private long sessionEndTargetTime = -1, fastNewOrderCounter, recentTpmC = 0, recentTpmTotal = 0;
    private long newOrderCounter = 0, sessionStartTimestamp, sessionEndTimestamp, sessionNextTimestamp = 0, sessionNextKounter = 0;

    private ThroughputPassiveMonitor transactionCountMonitor;
    private ThroughputPassiveMonitor newOrderCountMonitor;
    private ThroughputPassiveMonitor globalTransCountMonitor;
    private ThroughputPassiveMonitor localTransCountMonitor;
    private boolean signalTerminalsRequestEndSent = false, databaseDriverLoaded = false;
    private Object counterLock = new Object();

    private int transactionNum;
    private int terminalNum;

    private TpccRandom rnd;
    private int warehouseCount;

    public TpccClient(int clientId, String systemConfigFile, String partitioningFile, String dataFile, int terminalNum, int transactionNum, int warehouseCount, int wNewOrder, int wPayment, int wDelivery, int wOrderStatus, int wStockLevel, String terminalDistribution) {
        TpccProcedure appProcedure = new TpccProcedure();
        clientProxy = new Client(clientId, systemConfigFile, partitioningFile, appProcedure);
        this.preLoadData(dataFile);
        appProcedure.init("CLIENT", clientProxy.getCache(), clientProxy.getSecondaryIndex(), logger, 0);
        gen = new Random(System.nanoTime());
        this.terminalNum = terminalNum;
        this.transactionNum = transactionNum;
        this.warehouseCount = warehouseCount;
        if (terminalNum == 0) {
            terminals = new TpccTerminal[1];
            terminals[0] = new TpccTerminal(clientProxy, "Interactive", 100, warehouseCount, 1, 1, 100, 45, 43, 4, 4, 4, -1, this, new BenchContext.CallbackHandler(this.clientProxy.getId()));
            terminals[0].setLogger(logger);
            this.runInteractive();
        } else {

            this.runAuto(wNewOrder, wPayment, wDelivery, wOrderStatus, wStockLevel, "w=1:d=1_w=2:d=1_w=1:d=2_w=2:d=2_w=1:d=3_w=2:d=3_w=1:d=4_w=2:d=4_w=1:d=5_w=2:d=5");
        }
    }

    public static void main(String[] args) {
        if (args.length != 11 && args.length != 17) {//
            System.out.println("USAGE: AppClient | clientId | systemConfigFile | partitionConfigFile | dataFile | terminalNumber(0 for interactive) | transaction count | wNewOrder | wPayment | wOrderStatus | wDelivery | wStockLevel");
            System.exit(1);
        }
        int index = 0;
        int clientId = Integer.parseInt(args[index++]);
        String systemConfigFile = args[index++];
        String partitionsConfigFile = args[index++];
        String dataFile = args[index++];
        int terminalNumber = Integer.parseInt(args[index++]);
        int transactionNum = Integer.parseInt(args[index++]);
        int wNewOrder = Integer.parseInt(args[index++]);
        int wPayment = Integer.parseInt(args[index++]);
        int wDelivery = Integer.parseInt(args[index++]);
        int wOrderStatus = Integer.parseInt(args[index++]);
        int wStockLevel = Integer.parseInt(args[index++]);
        String terminalDistribution = null;
        if (args.length == 17) {
            String gathererNode = args[index++];
            int gathererPort = Integer.parseInt(args[index++]);
            String fileDirectory = args[index++];
            int experimentDuration = Integer.parseInt(args[index++]);
            int warmUpTime = Integer.parseInt(args[index++]);
            DataGatherer.configure(experimentDuration, fileDirectory, gathererNode, gathererPort, warmUpTime);
            terminalDistribution = args[index++];
        }
        System.out.println(clientId + " - " + terminalDistribution);
        String[] dataFileParts = dataFile.split("/");
        int warehouseCount = Integer.parseInt(dataFileParts[dataFileParts.length - 1].split("_")[1]);
        TpccClient appcli = new TpccClient(clientId, systemConfigFile, partitionsConfigFile, dataFile, terminalNumber, transactionNum, warehouseCount, wNewOrder, wPayment, wDelivery, wOrderStatus, wStockLevel, terminalDistribution);
//        appcli.preLoadData(dataFile);

    }

//    private void preLoadData(String file) {
//
//        String redisHost = "192.168.3.90";
//        boolean cacheLoaded = false;
//        String[] fileNameParts = file.split("/");
//        String fileName = fileNameParts[fileNameParts.length - 1];
//        Integer partitionCount = Partition.getPartitionsCount();
//        System.out.println("[CLIENT-" + this.clientProxy.getId() + "] Creating connection to redis host " + redisHost);
//        Jedis jedis = new Jedis(redisHost);
//        Codec codec = new CodecUncompressedKryo();
//        long start = System.currentTimeMillis();
//        String keyObjectGraph = fileName + "_p_" + partitionCount + "_CLIENT_" + this.clientProxy.getId() + "_objectGraph";
//        String keySecondaryIndex = fileName + "_p_" + partitionCount + "_CLIENT_" + this.clientProxy.getId() + "_secondaryIndex";
//        String keyDataLoaded = fileName + "_p_" + partitionCount + "_CLIENT_" + this.clientProxy.getId() + "_data_loaded";
//
//        try {
//            String cached = jedis.get(keyDataLoaded);
//            if (cached != null && cached.equals("OK")) {
//                System.out.println("[CLIENT-" + this.clientProxy.getId() + "] loading sample data from cache...");
//                this.clientProxy.setCache((PRObjectGraph) codec.createObjectFromString(jedis.get(keyObjectGraph)));
//                this.clientProxy.setSecondaryIndex((HashMap<String, Set<ObjId>>) codec.createObjectFromString(jedis.get(keySecondaryIndex)));
//                cacheLoaded = true;
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.exit(-1);
//        }
//        if (!cacheLoaded) {
//            System.out.println("[CLIENT-" + this.clientProxy.getId() + "] loading sample data from file...");
//            TpccUtil.loadDataToCache(file, this.clientProxy.getCache(), this.clientProxy.getSecondaryIndex(), (objId, obj) -> {
//                int dest = TpccUtil.mapIdToPartition(objId);
//                PRObjectNode node = new PRObjectNode(objId, dest);
//                this.clientProxy.getCache().addNode(node);
//            });
//            jedis.set(keyObjectGraph, codec.getString(this.clientProxy.getCache()));
//            jedis.set(keySecondaryIndex, codec.getString(this.clientProxy.getSecondaryIndex()));
//            jedis.set(keyDataLoaded, "OK");
//        }
//        System.out.println("[CLIENT-" + this.clientProxy.getId() + "] Data loaded, takes " + (System.currentTimeMillis() - start));
//    }

    private void preLoadData(String file) {
        String hostName = null, redisHost;
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        if (hostName.indexOf("node") == 0) {
            redisHost = "192.168.3.91";
        } else {
            redisHost = "127.0.0.1";
        }
        boolean cacheLoaded = false;
        String[] fileNameParts = file.split("/");
        String fileName = fileNameParts[fileNameParts.length - 1];
        Integer partitionCount = Partition.getPartitionsCount();
        System.out.println("[CLIENT-" + this.clientProxy.getId() + "] Creating connection to redis host " + redisHost);
        BinaryJedis jedis = new BinaryJedis(redisHost);
        Codec codec = new CodecUncompressedKryo();
        long start = System.currentTimeMillis();
        byte[] keyObjectGraph = (fileName + "_p_" + partitionCount + "_CLIENT_" + this.clientProxy.getId() + "_objectGraph").getBytes();
        byte[] keySecondaryIndex = (fileName + "_p_" + partitionCount + "_CLIENT_" + this.clientProxy.getId() + "_secondaryIndex").getBytes();
        byte[] keyDataLoaded = (fileName + "_p_" + partitionCount + "_CLIENT_" + this.clientProxy.getId() + "_data_loaded").getBytes();

        try {
            byte[] cached = jedis.get(keyDataLoaded);
            if (cached != null && new String(cached).equals("OK")) {
                System.out.println("[CLIENT-" + this.clientProxy.getId() + "] loading sample data from cache...");
                this.clientProxy.setCache((PRObjectGraph) codec.createObjectFromBytes(jedis.get(keyObjectGraph)));
                this.clientProxy.setSecondaryIndex((HashMap<String, Set<ObjId>>) codec.createObjectFromBytes(jedis.get(keySecondaryIndex)));
                cacheLoaded = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        if (!cacheLoaded) {
            System.out.println("[CLIENT-" + this.clientProxy.getId() + "] loading sample data from file...");
            TpccUtil.loadDataToCache(file, this.clientProxy.getCache(), this.clientProxy.getSecondaryIndex(), (objId, obj) -> {
                int dest = TpccUtil.mapIdToPartition(objId);
                PRObjectNode node = new PRObjectNode(objId, dest);
                this.clientProxy.getCache().addNode(node);
            });
            jedis.set(keyObjectGraph, codec.getBytes(this.clientProxy.getCache()));
            jedis.set(keySecondaryIndex, codec.getBytes(this.clientProxy.getSecondaryIndex()));
            jedis.set(keyDataLoaded, new String("OK").getBytes());
        }
        System.out.println("[CLIENT-" + this.clientProxy.getId() + "] Data loaded, takes " + (System.currentTimeMillis() - start));
    }


    private void execute(int transaction) {
        transactionCount++;
        long now = System.currentTimeMillis();
        Consumer then = new Consumer() {
            @Override
            public void accept(Object o) {
                System.out.println("Time: " + (System.currentTimeMillis() - now) + " - " + o);
            }
        };
        switch (transaction) {
            case NEW_ORDER: {
                terminals[0].doNewOrder(then);
                break;
            }
            case PAYMENT: {
                terminals[0].doPayment(then);
                break;
            }
            case ORDER_STATUS: {
                terminals[0].doOrderStatus(then);
                break;
            }
            case DELIVERY: {
                terminals[0].doDelivery(then);
                break;
            }
            case STOCK_LEVEL: {
                terminals[0].doStockLevel(then);
                break;
            }

        }
    }


    public void runAuto(int iNewOrderWeight, int iPaymentWeight, int iDeliveryWeight, int iOrderStatusWeight, int iStockLevelWeight) {
        terminals = new TpccTerminal[this.terminalNum];
        terminalNames = new String[this.terminalNum];
        terminalsStarted = this.terminalNum;
        rnd = new TpccRandom(1);
        int[][] usedTerminals = new int[this.warehouseCount][TpccConfig.configDistPerWhse];
        for (int i = 0; i < this.warehouseCount; i++)
            for (int j = 0; j < TpccConfig.configDistPerWhse; j++)
                usedTerminals[i][j] = 0;

        BenchContext.CallbackHandler callbackHandler = new BenchContext.CallbackHandler(this.clientProxy.getId());

        for (int i = 0; i < this.terminalNum; i++) {
            int terminalWarehouseID;
            int terminalDistrictID;
            do {
                terminalWarehouseID = rnd.nextInt(1, this.warehouseCount);
                terminalDistrictID = rnd.nextInt(1, TpccConfig.configDistPerWhse);
            }
            while (usedTerminals[terminalWarehouseID - 1][terminalDistrictID - 1] == 1);
            usedTerminals[terminalWarehouseID - 1][terminalDistrictID - 1] = 1;

            String terminalName = this.clientProxy.getId() + "/Term-" + (i >= 9 ? "" + (i + 1) : "0" + (i + 1));

            TpccTerminal terminal = new TpccTerminal(this.clientProxy, terminalName, i, this.warehouseCount,
                    terminalWarehouseID, terminalDistrictID, transactionNum, iNewOrderWeight, iPaymentWeight, iDeliveryWeight, iOrderStatusWeight, iStockLevelWeight,
                    -1, this, callbackHandler);
//            System.out.println("Client " + this.clientProxy.getId() + " starting terminal for warehouse/district " + terminalWarehouseID + "/" + terminalDistrictID);
            terminal.setLogger(logger);
            terminals[i] = terminal;
            terminalNames[i] = terminalName;
            logger.debug(terminalName + "\t" + terminalWarehouseID);
        }
//        sessionEndTargetTime = executionTimeMillis;
        signalTerminalsRequestEndSent = false;

        synchronized (terminals) {
            logger.debug("Starting all terminals... Terminal count: " + terminals.length);
            transactionCount = 1;
            for (int i = 0; i < terminals.length; i++)
                (new Thread(terminals[i])).start();

        }

    }

    public void runAuto(int iNewOrderWeight, int iPaymentWeight, int iDeliveryWeight, int iOrderStatusWeight, int iStockLevelWeight, String terminalDistribution) {
        String[] terminalParts = terminalDistribution.split("_");
        terminals = new TpccTerminal[terminalParts.length];
        terminalNames = new String[terminalParts.length];
        terminalsStarted = terminalParts.length;
        BenchContext.CallbackHandler callbackHandler = new BenchContext.CallbackHandler(this.clientProxy.getId());
        for (int i = 0; i < terminalParts.length; i++) {
            String terminalName = this.clientProxy.getId() + "/Term-" + terminalParts[i];
            String[] terminalDetail = terminalParts[i].split(":");
            int terminalWarehouseID = Integer.parseInt(terminalDetail[0].split("=")[1]);
            int terminalDistrictID = Integer.parseInt(terminalDetail[1].split("=")[1]);
            TpccTerminal terminal = new TpccTerminal(this.clientProxy, terminalName, i, this.warehouseCount,
                    terminalWarehouseID, terminalDistrictID, transactionNum, iNewOrderWeight, iPaymentWeight, iDeliveryWeight, iOrderStatusWeight, iStockLevelWeight,
                    -1, this, callbackHandler);
            System.out.println("Client " + this.clientProxy.getId() + " starting terminal for warehouse/district " + terminalWarehouseID + "/" + terminalDistrictID);
            terminal.setLogger(logger);
            terminals[i] = terminal;
            terminalNames[i] = terminalName;
            logger.debug(terminalName + "\t" + terminalWarehouseID);
        }
//        sessionEndTargetTime = executionTimeMillis;
        signalTerminalsRequestEndSent = false;

        synchronized (terminals) {
            logger.debug("Starting all terminals... Terminal count: " + terminals.length);
            transactionCount = 1;
            for (int i = 0; i < terminals.length; i++)
                (new Thread(terminals[i])).start();

        }

    }

    public void runInteractive() {
        System.out.println("input format: r(ead)/n(ew order)/p(ayment)/order (s)tatus/d(elivery)/stock (l)evel/a(uto)/(or end to finish)");
        Scanner scan = new Scanner(System.in);
        String input;
        input = scan.nextLine();
        try {
            while (!input.equalsIgnoreCase("end")) {
                String[] params = input.split(" ");
                String opStr = params[0];
                if (opStr.equalsIgnoreCase("n")) {
                    execute(NEW_ORDER);
                } else if (opStr.equalsIgnoreCase("p")) {
                    execute(PAYMENT);
                } else if (opStr.equalsIgnoreCase("s")) {
                    execute(ORDER_STATUS);
                } else if (opStr.equalsIgnoreCase("d")) {
                    execute(DELIVERY);
                } else if (opStr.equalsIgnoreCase("l")) {
                    execute(STOCK_LEVEL);
                } else if (opStr.equalsIgnoreCase("r")) {
                    //r District:d_w_id=2:d_id=1
                    //r District:d_w_id=1:d_id=1
                    //
                    Map<String, Object> payload = new HashMap<>();
                    payload.put("key", params[1]);
                    Command cmd = new Command(TpccCommandType.READ, payload);
                    CompletableFuture<Message> cmdEx = clientProxy.executeCommand(cmd);
                    Message ret = cmdEx.get();
                    System.out.println(ret);
                } else if (opStr.equalsIgnoreCase("a")) {
                    for (int i = 0; i < 5000; i++) {
                        execute(NEW_ORDER);
                    }
                }
                input = scan.nextLine();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        scan.close();

    }

    public void signalTerminalEnded(TpccTerminal tpccTerminal, int newOrderCounter) {
    }


    public void signalTerminalEndedTransaction(int transactionNum, String terminalName, String transactionType, long executionTime, String comment, int newOrder) {
        transactionCount++;
        fastNewOrderCounter += newOrder;
        if (transactionNum == 1) {
            sessionStartTimestamp = System.currentTimeMillis();
        }
        if (transactionNum >= 1) {
            if (sessionEndTargetTime != -1 && System.currentTimeMillis() > sessionEndTargetTime) {
                signalTerminalsRequestEnd(true);
            }
//            updateStatusLine();
        }
    }


    private void signalTerminalsRequestEnd(boolean timeTriggered) {
        synchronized (terminals) {
            if (!signalTerminalsRequestEndSent) {
                if (timeTriggered)
                    logger.debug("The time limit has been reached.");
                logger.debug("Signalling all terminals to stop...");
                signalTerminalsRequestEndSent = true;

                for (int i = 0; i < terminals.length; i++)
                    if (terminals[i] != null)
                        terminals[i].stopRunningWhenPossible();

                logger.debug("Waiting for all active transactions to end...");
            }
        }
    }


    synchronized private void updateStatusLine() {
        updateStatusLine(this.clientProxy.getId() + "/" + "Term-00");
    }

    synchronized private void updateStatusLine(String terminalName) {
        long currTimeMillis = System.currentTimeMillis();

        if (currTimeMillis > sessionNextTimestamp) {
            StringBuilder informativeText = new StringBuilder("");
            Formatter fmt = new Formatter(informativeText);
            double tpmC = (6000000 * fastNewOrderCounter / (currTimeMillis - sessionStartTimestamp)) / 100.0;
            double tpmTotal = (6000000 * transactionCount / (currTimeMillis - sessionStartTimestamp)) / 100.0;

            sessionNextTimestamp += 1000;  /* update this every seconds */

            fmt.format(terminalName + ", Running Average tpmTOTAL: %.2f", tpmTotal);

            /* XXX What is the meaning of these numbers? */
            recentTpmC = (fastNewOrderCounter - sessionNextKounter) * 12;
            recentTpmTotal = (transactionCount - sessionNextKounter) * 12;
            sessionNextKounter = fastNewOrderCounter;
            fmt.format("    Current tpmTOTAL: %d", recentTpmTotal);

            long freeMem = Runtime.getRuntime().freeMemory() / (1024 * 1024);
            long totalMem = Runtime.getRuntime().totalMemory() / (1024 * 1024);
            fmt.format("    Memory Usage: %dMB / %dMB          ", (totalMem - freeMem), totalMem);

            logger.info(informativeText.toString());
//            for (int count = 0; count < 1 + informativeText.length(); count++)
//                System.out.print("\b");
        }
    }
}

