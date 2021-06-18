import {EventEmitter} from "events";

import {IClientPublishOptions, IClientSubscribeOptions, ISubscriptionGrant} from "mqtt";
import {QueueClient, QueueClientConnectionError} from "./queue-client";

export type TopicMessage = Buffer | string;
export type TopicPublisher = (request: ITopicPublishRequest) => Promise<IPublishResponse>;
export type TopicSubscriber = (request: ITopicSubscribeRequest) => Promise<ISubscribeResponse>;
export type TopicUnsubscriber = (topicPath: string) => Promise<IUnsubscribeResponse>;

export interface ITopicPublishRequest {
    topicPath: string;
    message: TopicMessage;
    options?: IClientPublishOptions
}

export interface ITopicSubscribeRequest {
    topicPath: string;
    options?: IClientSubscribeOptions;
}

export interface IPublishResponse {
    published: boolean;
}

export interface ISubscribeResponse {
    subscribed: boolean;
    subscriptionInfo?: ISubscriptionGrant;
}

export interface IUnsubscribeResponse {
    unsubscribed: boolean;
}

export interface ITopicConstructor {
    publisher: TopicPublisher;
    subscriber: TopicSubscriber;
    unsubscriber?: TopicUnsubscriber;
    topicPath: string;
    queueClient: QueueClient;
    encoding?: BufferEncoding;
}

export interface Topic {
    on(event: 'message', listener: (message: Buffer) => void): this;
    on(event: 'stringMessage', listener: (message: string) => void): this;
    on(event: 'jsonMessage', listener: (message: Object) => void): this;
}

export class Topic extends EventEmitter {

    private readonly _publisher: TopicPublisher;
    private readonly _subscriber: TopicSubscriber;
    private readonly _unsubscriber?: TopicUnsubscriber;

    private readonly _topicPath: string;
    private readonly _queueClient: QueueClient;
    private readonly _encoding: BufferEncoding;

    private _isSubscribed: boolean = false;

    private _noOfReceivedMessages: number = 0;
    private _noOfPublishedMessages: number = 0;

    constructor(options: ITopicConstructor) {
        super();

        this._publisher = options.publisher;
        this._subscriber = options.subscriber;
        this._unsubscriber = options.unsubscriber;
        this._topicPath = options.topicPath;
        this._queueClient = options.queueClient;
        this._encoding = options.encoding || 'utf-8';
    }


    public async publish(request: Omit<ITopicPublishRequest, 'topicPath'>): Promise<IPublishResponse> {

        if(!this.isConnected) {
            throw new QueueClientConnectionError('QueueClient/Topic not connected');
        }

        const publishResponse = await this._publisher({
            ...request,
            topicPath: this._topicPath
        });

        if(publishResponse.published) this._noOfPublishedMessages++;

        return publishResponse;
    }

    public async subscribe(options?: IClientSubscribeOptions): Promise<ISubscribeResponse> {

        if(!this.isConnected) {
            throw new QueueClientConnectionError('QueueClient/Topic not connected');
        }

        const subscribeResponse = await this._subscriber({
            topicPath: this._topicPath,
            options
        });

        this._isSubscribed = subscribeResponse.subscribed;

        return subscribeResponse;
    }

    public async unsubscribe(): Promise<IUnsubscribeResponse> {

        if(!this.isConnected) {
            throw new QueueClientConnectionError('QueueClient/Topic not connected');
        }

        if(this._unsubscriber) {
            const unsubscribeResponse = await this._unsubscriber(this._topicPath);
            this._isSubscribed = !unsubscribeResponse.unsubscribed;
            return unsubscribeResponse;
        } else {
            return {
                unsubscribed: false
            };
        }

    }

    public emitMessage(message: Buffer) {
        if(this.isSubscribed) {
            this._noOfReceivedMessages++;

            const messageAsString = message.toString(this._encoding);

            this.emit('message', message);
            this.emit('stringMessage', messageAsString);

            try {
                this.emit('jsonMessage', JSON.parse(messageAsString))
            } catch(err) {
                this.emit('jsonMessage', {invalidJson: true})
            }
        }
    }

    public get isSubscribed(): boolean {
        return this._isSubscribed;
    }

    public get isConnected(): boolean {
        return this._queueClient.isConnected
    }

    public get noOfPublishedMessages(): number {
        return this._noOfPublishedMessages;
    }

    public get noOfReceivedMessages(): number {
        return this._noOfReceivedMessages;
    }
}