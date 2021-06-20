import {Topic, TopicPublisher, TopicSubscriber, TopicUnsubscriber} from "./topic";
import type {IClientSubscribeOptions} from "mqtt";
import {QueueClient} from "./queue-client";

export interface Topics {
    [topicName: string]: Topic;
}

export type DefaultTopics = { [topicPath: string]: IAddTopicOptions };

export interface IAddTopicOptions {
    subscribe?: boolean;
    subscribeOptions?: IClientSubscribeOptions;
    encoding?: BufferEncoding;
}

export interface ITopicManagerConstructor {
    getPublisher: () => TopicPublisher;
    getSubscriber: () => TopicSubscriber;
    getUnsubscriber: () => (TopicUnsubscriber | null);
    queueClient: QueueClient;
    topics?: DefaultTopics;
}

export class TopicManager {

    private _topics: Topics = {};

    private readonly _getPublisher: () => TopicPublisher;
    private readonly _getSubscriber: () => TopicSubscriber;
    private readonly _getUnsubscriber: () => (TopicUnsubscriber | null);
    private readonly _queueClient: QueueClient;

    private _addDefaultTopicsTimeout: NodeJS.Timeout;

    constructor(options: ITopicManagerConstructor) {
        this._getPublisher = options.getPublisher;
        this._getSubscriber = options.getSubscriber;
        this._getUnsubscriber = options.getUnsubscriber;
        this._queueClient = options.queueClient;

        if(options.topics && Object.keys(options.topics).length > 0) {
            this._addDefaultTopics(options.topics);
        }
    }

    private _addDefaultTopics(topics: DefaultTopics) {

        const addTopics = async () => {
            if(this.isConnected) {
                clearTimeout(this._addDefaultTopicsTimeout);

                await Promise.all(
                    Object
                        .keys(topics)
                        .map(topicPath => this.addTopic(topicPath, topics[topicPath]))
                )

            } else {
                this._addDefaultTopicsTimeout = setTimeout(() => addTopics(), 10)
            }
        }

        addTopics();

    }

    public async addTopic(topicPath: string, options?: IAddTopicOptions): Promise<Topic> {
        const topic = new Topic({
            publisher: this._getPublisher(),
            subscriber: this._getSubscriber(),
            unsubscriber: this._getUnsubscriber(),
            queueClient: this._queueClient,
            encoding: options.encoding,
            topicPath
        });

        if(options?.subscribe) {
            await topic.subscribe(options?.subscribeOptions);
        }

        this._topics[topicPath] = topic;

        return topic;
    }

    public async removeTopic(topicName: string) {
        delete this._topics[topicName];
    }

    public get topics() {
        return this._topics;
    }

    public hasTopic(topicPath: string) {
        return this._topics.hasOwnProperty(topicPath);
    }

    public onMessage(topicPath: string, message: Buffer) {
        const topic = this._topics[topicPath];
        if(topic) topic.emitMessage(message);
    }

    get isConnected(): boolean {
        return this._queueClient.isConnected;
    }
}