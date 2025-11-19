export type AuthUser = {
  id: number;
  key: string;
  user_name: string;
  credits: number;
};

declare module 'fastify' {
  interface FastifyRequest {
    authUser?: AuthUser;
    requestStart?: number;
  }
}
