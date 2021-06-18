import type {IClientPublishOptions, IClientSubscribeOptions} from "mqtt";
import process from "process";
import {Topic, TopicMessage, TopicPublisher, TopicSubscriber, TopicUnsubscriber} from "./topic";
import {TopicManager} from "./topic-manager";

export interface ICreateQueueClient {
    waitForConnection?: boolean;
    waitForConnectionTimeout?: number;
    host: string;
    port: number;
    user?: string;
    password?: string;
}

export interface IPublishResponse {
    published: boolean;
}

export interface IPublishRequest {
    topicName: string;
    message: string | Buffer;
    options?: IClientPublishOptions
}

export interface IAddTopicOptions {
    subscribe?: boolean;
    subscribeOptions?: IClientSubscribeOptions;
}

export interface Topics {
    [topicName: string]: Topic;
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

    protected constructor(options: ICreateQueueClient) {
        this.host = options.host;
        this.port = options.port;

        this._topicManager = new TopicManager({
            queueClient: this,
            getSubscriber: () => this.subscriber,
            getPublisher: () => this.publisher,
            getUnsubscriber: () => this.unsubscriber
        })

        process.on('exit', () => this.close())
    }

    static async create(options: ICreateQueueClient): Promise<QueueClient> {
        return null;
    }

    public abstract close(): Promise<boolean>;
    public abstract isConnected: boolean;

    public get topicManager(): TopicManager {
        return this._topicManager;
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