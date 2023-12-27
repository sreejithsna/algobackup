TERM=xterm

echo "#################################"
echo " TRADE DAEMONS                  "
echo "#################################"
> /home/opc/mydhan/edelweiss.py.out
> /home/opc/mydhan/edel_orders.py.out
> /home/opc/mydhan/kite_option_chain.py.out

if [ $1 == "start" ]
then 
    echo "Starting Trade Daemons..."
    python3 /home/opc/mydhan/fetchToken.py

    start=1
    end=0
    while [ $start -eq 1 ] && [ $end -eq 0 ]
    do
        currenttime=$(date +%H:%M)
        echo "Current time =>" $currenttime

        #EDELWEISS MAIN DAEMON
        edelweiss=`ps -ef | grep edelweiss.py | grep -v grep | wc -l`
        if [ $edelweiss -eq 0 ] 
        then
            echo "EDELWEISS down!!"
            python3 /home/opc/mydhan/edelweiss.py >> /home/opc/mydhan/edelweiss.py.out &
            sleep 30
            tail -10 /home/opc/mydhan/edelweiss.py.out
        else
            echo "EDELWEISS up..."
        fi

        #EDEL ORDER DAEMON
        edel_orders=`ps -ef | grep edel_orders | grep -v grep | wc -l`
        if [ $edel_orders -eq 0 ] 
        then
            echo "EDEL Order Daemon down!!"
            python3 /home/opc/mydhan/edel_orders.py >> /home/opc/mydhan/edel_orders.py.out &
            tail -10 /home/opc/mydhan/edel_orders.py.out
        else    
            echo "EDEL Order Daemon up..."
        fi

        #KITE OPTION CHAIN DAEMON
        kite=`ps -ef | grep kite_option_chain.py | grep -v grep | wc -l`
        if [ $kite -eq 0 ]
        then
            echo "KITE OPTION CHAIN Daemon down!!"
            python3 /home/opc/mydhan/kite_option_chain.py >> /home/opc/mydhan/kite_option_chain.py.out &
            tail -10 /home/opc/mydhan/kite_option_chain.py.out
        else    
            echo "KITE OPTION CHAIN Daemon up..."
        fi

        #DHAN DAEMON
        kite=`ps -ef | grep dhan.py | grep -v grep | wc -l`
        if [ $kite -eq 0 ]
        then
            echo "DHAN Daemon down!!"
            python3 /home/opc/mydhan/dhan.py >> /home/opc/mydhan/dhan.py.out &
            tail -10 /home/opc/mydhan/dhan.py.out
        else    
            echo "DHAN Daemon up..."
        fi
        
        #Exit if time is greater than 3:29 PM, wait and stop services
        if [[ "$currenttime" > "15:28" ]]; then
            end=1
            echo "POWERING OFF..."
            sleep 300
            sudo systemctl stop edel.service
            #sudo shutdown --halt +5
            break
        else
            sleep 10
        fi
        
    done
fi


if [ $1 = "stop" ]
then
    echo "Killing Trading Daemons.."
    sleep 2
    kill -9 $(pgrep -f kite_option_chain)
    sleep 2
    kill -9 $(pgrep -f edelweiss.py)
    sleep 2
    kill -9 $(pgrep -f edel_orders.py)
    sleep 2
    kill -9 $(pgrep -f dhan.py)
    sleep 2
    ps -ef | grep *.py
fi
exit