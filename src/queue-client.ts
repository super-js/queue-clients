import process from "process";
import {TopicPublisher, TopicSubscriber, TopicUnsubscriber, IPublishResponse} from "./topic";
import {TopicManager, DefaultTopics} from "./topic-manager";

export interface ICreateQueueClient {
    waitForConnection?: boolean;
    waitForConnectionTimeout?: number;
    host: string;
    port: number;
    user?: string;
    password?: string;
    topics?: DefaultTopics;
}

export class QueueClientConnectionError extends Error {
    constructor(msg: string) {
        super(msg);
    }
}

export abstract class QueueClient {

    private readonly _topicManager: TopicManager;

    protected readonly host: string;
    protected readonly port: number;

    protected abstract publisher: TopicPublisher;
    protected abstract subscriber: TopicSubscriber;
    protected abstract unsubscriber?: TopicUnsubscriber;

    protected _lastError?: Error;

    protected constructor(options: ICreateQueueClient) {
        this.host = options.host;
        this.port = options.port;

        this._topicManager = new TopicManager({
            queueClient: this,
            getSubscriber: () => this.subscriber,
            getPublisher: () => this.publisher,
            getUnsubscriber: () => this.unsubscriber,
            topics: options.topics
        })

        process.on('exit', () => this.close())
    }

    static async create(clientName: string, options: ICreateQueueClient): Promise<QueueClient> {
        return null;
    }

    public abstract close(): Promise<boolean>;
    public abstract isConnected: boolean;

    public get topicManager(): TopicManager {
        return this._topicManager;
    }

    public get lastErrorMessage(): string {
        return this._lastError ? this._lastError.message : null;
    }

    protected onMessage(topicPath: string, message: Buffer) {
        this._topicManager.onMessage(topicPath, message);
    }

    protected async waitForConnection(waitForConnectionTimeout: number = 30): Promise<boolean> {

        const isConnected = await new Promise<boolean>((resolve) => {

            let noOfTries = 0;

            const checkForConnectionStatus = () => {
                setTimeout(() => {
                    if(this.isConnected) return resolve(true);

                    noOfTries++;

                    if(noOfTries >= waitForConnectionTimeout) return resolve(false);

                    checkForConnectionStatus();
                }, 1000);
            }

            checkForConnectionStatus();

        });

        if(!isConnected) throw new QueueClientConnectionError(`Unable to connect to ${this.host}:${this.port}`)

        return isConnected;
    }

}